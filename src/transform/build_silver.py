#!/usr/bin/env python
"""Silver = cleaned, de-duplicated, current-state tables. This is where ACID MERGE INTO happens.

Design choices worth explaining in an interview:
  * hidden partitioning months(<date col>)  -> queries filter on dates, never on partition columns
  * merge-on-read (write.merge.mode)        -> MERGE writes small delete files, compaction cleans up later
  * MERGE only updates when the incoming row is NEWER (update_date >= existing) -> late/duplicate safe
Usage: ./run.sh silver --batches 0     (default: all bronze batches - it is idempotent)"""
from __future__ import annotations

import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.spark import get_spark

PROPS = (
    "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read', "
    "'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read', "
    "'write.parquet.compression-codec'='zstd')"
)

TABLES = {
    "patient": {
        "keys": ["pat_id"],
        "ddl": f"""CREATE TABLE IF NOT EXISTS lake.silver.patient (
            pat_id STRING, pat_mrn_id STRING, pat_name STRING, birth_date DATE, sex STRING,
            state STRING, zip STRING, update_date TIMESTAMP, _batch_id INT
        ) USING iceberg {PROPS}""",
    },
    "pat_enc": {
        "keys": ["pat_enc_csn_id"],
        "ddl": f"""CREATE TABLE IF NOT EXISTS lake.silver.pat_enc (
            pat_enc_csn_id BIGINT, pat_id STRING, contact_date DATE, enc_type STRING, department_id BIGINT,
            visit_prov_id STRING, appt_status STRING, bp_systolic INT, bp_diastolic INT,
            hosp_admsn_time TIMESTAMP, hosp_disch_time TIMESTAMP, update_date TIMESTAMP, _batch_id INT
        ) USING iceberg PARTITIONED BY (months(contact_date)) {PROPS}""",
    },
    "pat_enc_dx": {
        "keys": ["pat_enc_csn_id", "line"],
        "ddl": f"""CREATE TABLE IF NOT EXISTS lake.silver.pat_enc_dx (
            pat_enc_csn_id BIGINT, line INT, pat_id STRING, dx_id BIGINT, icd10_code STRING, dx_name STRING,
            contact_date DATE, update_date TIMESTAMP, _batch_id INT
        ) USING iceberg PARTITIONED BY (months(contact_date)) {PROPS}""",
    },
    "order_proc": {
        "keys": ["order_proc_id"],
        "ddl": f"""CREATE TABLE IF NOT EXISTS lake.silver.order_proc (
            order_proc_id BIGINT, pat_enc_csn_id BIGINT, pat_id STRING, proc_code STRING, proc_name STRING,
            ordering_date DATE, order_status STRING, update_date TIMESTAMP, _batch_id INT
        ) USING iceberg PARTITIONED BY (months(ordering_date)) {PROPS}""",
    },
    "order_results": {
        "keys": ["order_proc_id", "line"],
        "ddl": f"""CREATE TABLE IF NOT EXISTS lake.silver.order_results (
            order_proc_id BIGINT, line INT, component_name STRING, ord_num_value DOUBLE, reference_unit STRING,
            result_date DATE, update_date TIMESTAMP, _batch_id INT
        ) USING iceberg PARTITIONED BY (months(result_date)) {PROPS}""",
    },
}


def merge_table(spark, name: str, cfg: dict, batches: list[int] | None) -> None:
    target = f"lake.silver.{name}"
    spark.sql(cfg["ddl"])
    src = spark.table(f"lake.bronze.{name}")
    if batches:
        src = src.where(F.col("_batch_id").isin(batches))
    w = Window.partitionBy(*cfg["keys"]).orderBy(F.col("update_date").desc(), F.col("_batch_id").desc())
    src = src.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    fields = spark.table(target).schema.fields
    src = src.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in fields])
    view = f"_src_{name}"
    src.createOrReplaceTempView(view)
    on = " AND ".join(f"t.{k} = s.{k}" for k in cfg["keys"])
    before = spark.table(target).count()
    spark.sql(
        f"""MERGE INTO {target} t USING {view} s ON {on}
            WHEN MATCHED AND s.update_date >= t.update_date THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *"""
    )
    after = spark.table(target).count()
    print(f"  {target:<28} rows {before:>9,} -> {after:>9,}  (source rows this run: {src.count():,})")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--batches", type=int, nargs="*", default=None)
    args = ap.parse_args()
    spark = get_spark("silver")
    print(f"MERGE bronze -> silver (batches: {args.batches or 'all'})")
    for name, cfg in TABLES.items():
        merge_table(spark, name, cfg, args.batches)
