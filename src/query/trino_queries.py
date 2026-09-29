#!/usr/bin/env python
"""Phase 9a - same Iceberg tables, different engine (Trino), no copy of data.
./run.sh trino-sql           engine queries, metadata tables, time travel, Trino WRITE that Spark can read
./run.sh trino-sql --rbac    prove least-privilege: analyst can read gold but not silver"""
from __future__ import annotations

import argparse

from trino.dbapi import connect


def cursor(catalog="iceberg", user="engineer", schema="gold"):
    return connect(host="trino", port=8080, user=user, catalog=catalog, schema=schema).cursor()


def q(cur, sql: str, limit: int = 12):
    print(f"\ntrino> {' '.join(sql.split())}")
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description] if cur.description else []
    if cols:
        print("  " + " | ".join(cols))
    for r in rows[:limit]:
        print("  " + " | ".join(str(x) for x in r))
    if len(rows) > limit:
        print(f"  ... {len(rows)} rows")
    return rows


def engine_demo() -> None:
    cur = cursor()
    q(cur, "SHOW CATALOGS")
    q(cur, "SELECT measure, denominator, numerator, rate FROM iceberg.gold.hedis_rates WHERE state = 'ALL' ORDER BY measure")
    q(cur, "SELECT state, count(*) AS patients FROM iceberg.gold.dim_patient GROUP BY state ORDER BY state")
    print("\n-- Iceberg metadata tables (Trino syntax: table$name) --")
    snaps = q(cur, 'SELECT snapshot_id, committed_at, operation FROM iceberg.gold."hedis_rates$snapshots" ORDER BY committed_at')
    q(cur, 'SELECT count(*) AS data_files, sum(file_size_in_bytes) AS bytes FROM iceberg.gold."fact_encounter$files"')
    appends = [s for s in snaps if s[2] == "append"]
    if appends:
        first = appends[0][0]
        print("\n-- time travel in Trino --")
        q(cur, f"SELECT measure, rate FROM iceberg.gold.hedis_rates FOR VERSION AS OF {first} WHERE state = 'ALL' ORDER BY measure")
    print("\n-- Trino WRITES an Iceberg table; Spark/DuckDB can read it (open format = no lock-in) --")
    q(cur, "DROP TABLE IF EXISTS iceberg.gold.trino_smoke")
    q(cur, "CREATE TABLE iceberg.gold.trino_smoke AS SELECT measure, rate FROM iceberg.gold.hedis_rates WHERE state = 'ALL'")
    q(cur, "SELECT * FROM iceberg.gold.trino_smoke ORDER BY measure")
    print("\nNow verify from Spark:  ./run.sh shell  ->  python -c \"from common.spark import get_spark; "
          "get_spark().sql('select * from lake.gold.trino_smoke').show()\"")


def rbac_demo() -> None:
    cur = cursor(catalog="iceberg_analyst", user="analyst", schema="gold")
    print("Logged in as principal `analyst` (catalog iceberg_analyst)")
    for label, sql in [
        ("gold  (allowed)", "SELECT measure, rate FROM iceberg_analyst.gold.hedis_rates WHERE state = 'ALL' ORDER BY measure"),
        ("gold.dim_patient (allowed)", "SELECT count(*) FROM iceberg_analyst.gold.dim_patient"),
        ("silver.patient (should be DENIED)", "SELECT pat_name, birth_date FROM iceberg_analyst.silver.patient LIMIT 5"),
        ("bronze.patient (should be DENIED)", "SELECT * FROM iceberg_analyst.bronze.patient LIMIT 5"),
    ]:
        print(f"\n### {label}")
        try:
            q(cur, sql)
        except Exception as e:  # noqa: BLE001
            print(f"  DENIED/ERROR -> {str(e).splitlines()[0][:200]}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rbac", action="store_true")
    args = ap.parse_args()
    rbac_demo() if args.rbac else engine_demo()
