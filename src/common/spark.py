"""One place that builds the SparkSession wired to Polaris (REST) + Garage (S3).

Catalog alias inside Spark is `lake`  ->  tables are lake.<namespace>.<table>
NOTE: we do NOT send X-Iceberg-Access-Delegation. Garage has no STS, so no credential vending;
Spark uses the static S3 keys below (Polaris catalog has stsUnavailable=true).
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from common.config import CATALOG, POLARIS_URL, S3_ENDPOINT, S3_KEY, S3_REGION, S3_SECRET, creds

ICEBERG_VERSION = "1.10.0"
PACKAGES = ",".join(
    [
        f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}",
        f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    ]
)


def get_spark(app: str = "healthlake", role: str = "engineer", driver_memory: str = "4g") -> SparkSession:
    cid, secret = creds(role)
    c = "spark.sql.catalog.lake"
    spark = (
        SparkSession.builder.appName(app)
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", "lake")
        .config(c, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{c}.type", "rest")
        .config(f"{c}.uri", f"{POLARIS_URL}/api/catalog")
        .config(f"{c}.warehouse", CATALOG)
        .config(f"{c}.credential", f"{cid}:{secret}")
        .config(f"{c}.oauth2-server-uri", f"{POLARIS_URL}/api/catalog/v1/oauth/tokens")
        .config(f"{c}.scope", "PRINCIPAL_ROLE:ALL")
        .config(f"{c}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{c}.s3.endpoint", S3_ENDPOINT)
        .config(f"{c}.s3.path-style-access", "true")
        .config(f"{c}.s3.access-key-id", S3_KEY)
        .config(f"{c}.s3.secret-access-key", S3_SECRET)
        .config(f"{c}.client.region", S3_REGION)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
