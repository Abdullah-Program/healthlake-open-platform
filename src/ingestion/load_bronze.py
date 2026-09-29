#!/usr/bin/env python
"""Bronze = raw extracts landed as Iceberg, append-only, plus lineage columns.
Column names are lower-cased (Trino/Iceberg friendly). Re-running a batch is a no-op (idempotent).

Usage: ./run.sh bronze --batches 0        (initial load)
       ./run.sh bronze --batches 1 2 3    (incrementals)"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from common.config import ROOT
from common.spark import get_spark

TABLES = ["PATIENT", "PAT_ENC", "PAT_ENC_DX", "ORDER_PROC", "ORDER_RESULTS"]
RAW = ROOT / "data" / "raw"


def load(spark, table: str, batch: int) -> None:
    target = f"lake.bronze.{table.lower()}"
    exists = spark.catalog.tableExists(target)
    if exists and spark.table(target).where(F.col("_batch_id") == batch).limit(1).count():
        print(f"  = {target} batch {batch} already loaded, skipping")
        return
    src = RAW / table / f"batch={batch:02d}"
    df = spark.read.parquet(str(src))
    df = (
        df.toDF(*[c.lower() for c in df.columns])
        .withColumn("_batch_id", F.lit(batch))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
    w = df.writeTo(target).using("iceberg").tableProperty("format-version", "2")
    if exists:
        w.append()
    else:
        w.partitionedBy(F.col("_batch_id")).create()
    print(f"  + {target} <- batch {batch}: {df.count():,} rows")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--batches", type=int, nargs="+", default=[0, 1, 2, 3])
    args = ap.parse_args()
    spark = get_spark("bronze")
    for b in args.batches:
        print(f"batch {b}")
        for t in TABLES:
            load(spark, t, b)
