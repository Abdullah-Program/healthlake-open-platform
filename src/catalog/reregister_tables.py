#!/usr/bin/env python
"""Recovery for the in-memory Polaris: after a restart the catalog forgets its tables, but every Iceberg table
is still sitting in Garage (data/ + metadata/). This walks the bucket, finds the newest *.metadata.json of each
table and re-registers it. Great way to SEE that an Iceberg catalog is just a pointer to the latest metadata file.

Order: ./run.sh catalog  ->  ./run.sh trino  ->  ./run.sh reregister  (then ./run.sh rbac again if you used it)"""
from __future__ import annotations

import re

import boto3
from botocore.config import Config

from common.config import S3_BUCKET, S3_ENDPOINT, S3_KEY, S3_REGION, S3_SECRET
from common.spark import get_spark

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_KEY,
    aws_secret_access_key=S3_SECRET,
    region_name=S3_REGION,
    config=Config(s3={"addressing_style": "path"}),
)

latest: dict[str, str] = {}  # table dir -> newest metadata key
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET, Prefix="warehouse/"):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".metadata.json") and "/metadata/" in key:
            table_dir = key.rsplit("/metadata/", 1)[0]
            if table_dir not in latest or key > latest[table_dir]:  # 00012-<uuid>... sorts by sequence number
                latest[table_dir] = key

spark = get_spark("reregister")
for table_dir, key in sorted(latest.items()):
    parts = table_dir.split("/")  # warehouse/<namespace>/<table>[-<uuid>]
    if len(parts) < 3:
        continue
    ns, table = parts[1], re.sub(r"-[0-9a-f]{32}$", "", parts[2])
    ident = f"{ns}.{table}"
    if spark.catalog.tableExists(f"lake.{ident}"):
        print(f"  = {ident} already registered")
        continue
    try:
        spark.sql(f"CALL lake.system.register_table(table => '{ident}', metadata_file => 's3://{S3_BUCKET}/{key}')")
        print(f"  + {ident} <- {key}")
    except Exception as e:  # noqa: BLE001
        print(f"  ! {ident}: {str(e).splitlines()[0]}")
