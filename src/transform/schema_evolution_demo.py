#!/usr/bin/env python
"""Phase 6 - schema + partition evolution WITHOUT rewriting data.
Works on a sandbox copy (silver.pat_enc_sandbox) so the real pipeline is never broken.

Proof we print:  the set of data files is byte-for-byte identical before and after the ALTERs."""
from __future__ import annotations

from common.spark import get_spark

SB = "lake.silver.pat_enc_sandbox"


def files(spark) -> set[str]:
    return {r.file_path for r in spark.sql(f"SELECT file_path FROM {SB}.files").collect()}


def snapshots(spark) -> int:
    return spark.sql(f"SELECT count(*) c FROM {SB}.snapshots").first().c


def main() -> None:
    spark = get_spark("schema-evolution")
    spark.sql(f"DROP TABLE IF EXISTS {SB} PURGE")
    spark.sql(
        f"""CREATE TABLE {SB} USING iceberg PARTITIONED BY (months(contact_date))
            TBLPROPERTIES ('format-version'='2') AS SELECT * FROM lake.silver.pat_enc"""
    )
    snap0 = spark.sql(f"SELECT snapshot_id FROM {SB}.snapshots ORDER BY committed_at DESC LIMIT 1").first().snapshot_id
    f0, s0 = files(spark), snapshots(spark)
    print(f"baseline: {len(f0)} data files, {s0} snapshot(s), snapshot_id={snap0}")

    print("\n-- metadata-only changes --")
    spark.sql(f"ALTER TABLE {SB} ADD COLUMN telehealth_flag BOOLEAN")
    spark.sql(f"ALTER TABLE {SB} RENAME COLUMN visit_prov_id TO rendering_prov_id")
    spark.sql(f"ALTER TABLE {SB} ALTER COLUMN bp_systolic TYPE BIGINT")  # int -> long widening is allowed
    spark.sql(f"ALTER TABLE {SB} ADD PARTITION FIELD bucket(8, pat_id)")  # partition evolution
    f1, s1 = files(spark), snapshots(spark)
    print(f"after ALTERs: {len(f1)} data files, {s1} snapshot(s)")
    assert f0 == f1, "data files changed - that would mean a rewrite!"
    print("PROOF: identical file set, no new snapshot -> schema/partition evolution touched only metadata")

    print("\n-- old rows read the new column as NULL --")
    spark.sql(f"SELECT enc_type, rendering_prov_id, bp_systolic, telehealth_flag FROM {SB} LIMIT 5").show()

    print("-- new writes use the new schema and the NEW partition spec --")
    spark.sql(f"INSERT INTO {SB} SELECT * FROM {SB} WHERE contact_date >= DATE '2025-12-01' LIMIT 500")
    spark.sql(f"UPDATE {SB} SET telehealth_flag = (enc_type = 'Telehealth') WHERE telehealth_flag IS NULL AND contact_date >= DATE '2025-12-01'")
    print("data files per partition-spec id (old files keep spec 0, new files use spec 1):")
    spark.sql(f"SELECT spec_id, count(*) AS files FROM {SB}.files GROUP BY spec_id ORDER BY spec_id").show()

    print("-- time travel still returns the ORIGINAL schema --")
    old = spark.sql(f"SELECT * FROM {SB} VERSION AS OF {snap0}")
    print("columns at baseline snapshot:", old.columns)
    print("columns now                 :", spark.table(SB).columns)
    print("\nTry the same table in Trino:  ./run.sh trino-cli  ->  DESCRIBE iceberg.silver.pat_enc_sandbox;")


if __name__ == "__main__":
    main()
