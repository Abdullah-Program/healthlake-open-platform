"""Peek into the bucket: shows what Iceberg actually wrote (data/ + metadata/)."""
import sys
from collections import Counter

import boto3
from botocore.config import Config

from common.config import S3_BUCKET, S3_ENDPOINT, S3_KEY, S3_REGION, S3_SECRET

prefix = sys.argv[1] if len(sys.argv) > 1 else "warehouse/"
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_KEY,
    aws_secret_access_key=S3_SECRET,
    region_name=S3_REGION,
    config=Config(s3={"addressing_style": "path"}),
)
count, size, kinds = 0, 0, Counter()
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET, Prefix=prefix):
    for obj in page.get("Contents", []):
        count += 1
        size += obj["Size"]
        kinds[obj["Key"].rsplit(".", 1)[-1]] += 1
        if count <= 15:
            print(f"{obj['Size']:>10,}  {obj['Key']}")
print(f"\n{count} objects, {size / 1e6:.1f} MB under s3://{S3_BUCKET}/{prefix}")
print("by extension:", dict(kinds))
