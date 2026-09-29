"""./run.sh smoke            -> checks Garage (S3) + Polaris login
   ./run.sh smoke --spark    -> also creates/drops a tiny Iceberg table via Spark (after ./run.sh catalog)"""
import sys

import boto3
import requests
from botocore.config import Config

from common.config import POLARIS_URL, S3_BUCKET, S3_ENDPOINT, S3_KEY, S3_REGION, S3_SECRET, env


def check_s3():
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
        region_name=S3_REGION,
        config=Config(s3={"addressing_style": "path"}),
    )
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    assert S3_BUCKET in buckets, f"bucket {S3_BUCKET} missing, have {buckets}"
    s3.put_object(Bucket=S3_BUCKET, Key="_smoke/hello.txt", Body=b"hello")
    assert s3.get_object(Bucket=S3_BUCKET, Key="_smoke/hello.txt")["Body"].read() == b"hello"
    s3.delete_object(Bucket=S3_BUCKET, Key="_smoke/hello.txt")
    print(f"[ok] Garage S3 reachable, bucket '{S3_BUCKET}' read/write works")


def check_polaris():
    r = requests.post(
        f"{POLARIS_URL}/api/catalog/v1/oauth/tokens",
        data={
            "grant_type": "client_credentials",
            "client_id": env("POLARIS_ROOT_CLIENT_ID", "root"),
            "client_secret": env("POLARIS_ROOT_CLIENT_SECRET", required=True),
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        timeout=20,
    )
    r.raise_for_status()
    print("[ok] Polaris token issued for root principal")


def check_spark():
    from common.spark import get_spark

    spark = get_spark("smoke")
    spark.sql("SHOW NAMESPACES").show()
    spark.sql("CREATE TABLE IF NOT EXISTS lake.bronze._smoke (id INT) USING iceberg")
    spark.sql("INSERT INTO lake.bronze._smoke VALUES (1), (2)")
    assert spark.table("lake.bronze._smoke").count() == 2
    spark.sql("DROP TABLE lake.bronze._smoke PURGE")
    print("[ok] Spark -> Polaris -> Garage round trip works")


if __name__ == "__main__":
    check_s3()
    check_polaris()
    if "--spark" in sys.argv:
        check_spark()
