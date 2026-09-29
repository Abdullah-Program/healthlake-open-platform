# Optional stretch: Snowflake as a 4th engine

Snowflake runs in the cloud, so it cannot reach `garage:3900` on your laptop. Free-forever option: skip it. The project already proves multi-engine with Spark + Trino + DuckDB.

If you still want it (needs a Snowflake trial account AND a real cloud bucket such as AWS S3):

1. Iceberg metadata stores absolute paths (`s3://healthlake/...`). Moving a table to another bucket needs a path rewrite: look up Iceberg's `rewrite_table_path` Spark procedure (Iceberg 1.8+).
2. Copy the rewritten table to the real bucket.
3. In Snowflake create an external volume for that bucket, then follow Snowflake's docs for Iceberg tables from object storage.

Treat this as a research task: read the current Snowflake + Iceberg docs first (syntax changes often). Do NOT claim Glue-vs-Snowflake cost or speed numbers unless you measured them yourself.
