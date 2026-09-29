#!/usr/bin/env python
"""Phase 7 - table maintenance: compaction, delete-file cleanup, manifest rewrite, snapshot expiry, orphan files.
Run it AFTER a few incremental loads (./run.sh pipeline 1..3) - that is what creates many small files.
NOTE: expire_snapshots removes old snapshots => time travel to them is gone. Run ./run.sh timetravel first.
hedis_* tables are skipped on purpose so their history stays for time-travel demos."""
from __future__ import annotations

import argparse
import datetime as dt

from common.spark import get_spark

TABLES = [
    "silver.pat_enc",
    "silver.pat_enc_dx",
    "silver.order_proc",
    "silver.order_results",
    "silver.patient",
]


def stats(spark, t: str) -> dict:
    data = spark.sql(f"SELECT count(*) AS n, coalesce(sum(file_size_in_bytes), 0) AS b FROM lake.{t}.data_files").first()
    dele = spark.sql(f"SELECT count(*) AS n FROM lake.{t}.delete_files").first().n
    snaps = spark.sql(f"SELECT count(*) AS n FROM lake.{t}.snapshots").first().n
    return {"data_files": data.n, "delete_files": dele, "MB": round(data.b / 1e6, 1), "snapshots": snaps}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", type=int, default=3, help="snapshots to retain")
    args = ap.parse_args()
    spark = get_spark("maintenance")
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    for t in TABLES:
        print(f"\n=== {t} ===")
        before = stats(spark, t)
        print("before:", before)

        spark.sql(
            f"""CALL lake.system.rewrite_data_files(table => '{t}', strategy => 'binpack',
                options => map('min-input-files', '2', 'delete-file-threshold', '1'))"""
        ).show(truncate=False)
        try:
            spark.sql(f"CALL lake.system.rewrite_position_delete_files(table => '{t}')").show(truncate=False)
        except Exception as e:  # noqa: BLE001
            print("  (rewrite_position_delete_files skipped:", str(e).splitlines()[0], ")")
        spark.sql(f"CALL lake.system.rewrite_manifests(table => '{t}')").show(truncate=False)
        spark.sql(f"CALL lake.system.expire_snapshots(table => '{t}', older_than => TIMESTAMP '{now}', retain_last => {args.keep})").show(truncate=False)
        try:
            spark.sql(f"CALL lake.system.remove_orphan_files(table => '{t}')").show(truncate=False)
        except Exception as e:  # noqa: BLE001
            print("  (remove_orphan_files skipped:", str(e).splitlines()[0], ")")

        after = stats(spark, t)
        print("after :", after)


if __name__ == "__main__":
    main()
