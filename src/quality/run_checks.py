#!/usr/bin/env python
"""Data-quality gate between silver and gold. Results are stored in lake.gold.dq_results (an Iceberg table!).
Exit code 1 if any check fails -> an orchestrator (Airflow etc.) can stop the pipeline."""
from __future__ import annotations

import datetime as dt
import sys

from common.spark import get_spark

CHECKS = [
    ("patient.pat_id is unique", "SELECT count(*) - count(DISTINCT pat_id) FROM lake.silver.patient"),
    ("pat_enc.csn is unique", "SELECT count(*) - count(DISTINCT pat_enc_csn_id) FROM lake.silver.pat_enc"),
    ("pat_enc_dx (csn,line) is unique", "SELECT count(*) - count(DISTINCT concat(pat_enc_csn_id, '-', line)) FROM lake.silver.pat_enc_dx"),
    ("order_proc.id is unique", "SELECT count(*) - count(DISTINCT order_proc_id) FROM lake.silver.order_proc"),
    ("patient.birth_date not null", "SELECT count(*) FROM lake.silver.patient WHERE birth_date IS NULL"),
    ("RI: pat_enc -> patient", "SELECT count(*) FROM lake.silver.pat_enc e LEFT ANTI JOIN lake.silver.patient p ON e.pat_id = p.pat_id"),
    ("RI: pat_enc_dx -> pat_enc", "SELECT count(*) FROM lake.silver.pat_enc_dx d LEFT ANTI JOIN lake.silver.pat_enc e ON d.pat_enc_csn_id = e.pat_enc_csn_id"),
    ("RI: order_proc -> pat_enc", "SELECT count(*) FROM lake.silver.order_proc o LEFT ANTI JOIN lake.silver.pat_enc e ON o.pat_enc_csn_id = e.pat_enc_csn_id"),
    ("RI: order_results -> order_proc", "SELECT count(*) FROM lake.silver.order_results r LEFT ANTI JOIN lake.silver.order_proc o ON r.order_proc_id = o.order_proc_id"),
    ("BP in plausible range", "SELECT count(*) FROM lake.silver.pat_enc WHERE bp_systolic IS NOT NULL AND (bp_systolic NOT BETWEEN 60 AND 260 OR bp_diastolic NOT BETWEEN 30 AND 160)"),
    ("HbA1c in plausible range", "SELECT count(*) FROM lake.silver.order_results WHERE component_name = 'HEMOGLOBIN A1C' AND ord_num_value NOT BETWEEN 3 AND 20"),
]


def main() -> None:
    spark = get_spark("dq")
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    rows, failed_any = [], False
    print(f"{'check':<40} {'failed rows':>12}  status")
    for name, sql in CHECKS:
        bad = int(spark.sql(sql).first()[0])
        status = "PASS" if bad == 0 else "FAIL"
        failed_any |= bad != 0
        rows.append((now, name, bad, status))
        print(f"{name:<40} {bad:>12,}  {status}")
    spark.sql(
        "CREATE TABLE IF NOT EXISTS lake.gold.dq_results "
        "(run_ts TIMESTAMP, check_name STRING, failed_rows BIGINT, status STRING) USING iceberg"
    )
    if rows:
        val_strs = []
        for ts, name, bad, status in rows:
            clean_name = name.replace("'", "''")
            val_strs.append(f"(TIMESTAMP '{ts.isoformat()}', '{clean_name}', {bad}, '{status}')")
        spark.sql(f"INSERT INTO lake.gold.dq_results VALUES {', '.join(val_strs)}")
    if failed_any:
        sys.exit(1)


if __name__ == "__main__":
    main()
