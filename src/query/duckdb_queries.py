#!/usr/bin/env python
"""Phase 9b - DuckDB (embedded, no server) reading the SAME tables through the Polaris REST catalog.
ACCESS_DELEGATION_MODE 'none' because Garage has no STS: we give DuckDB the S3 keys directly."""
from __future__ import annotations

from common.config import CATALOG, POLARIS_URL, S3_ENDPOINT, S3_KEY, S3_REGION, S3_SECRET, creds


def connect(role: str = "engineer"):
    import duckdb

    cid, secret = creds(role)
    host = S3_ENDPOINT.replace("http://", "").replace("https://", "")
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
    con.execute(
        f"""CREATE OR REPLACE SECRET polaris_secret (
              TYPE ICEBERG, CLIENT_ID '{cid}', CLIENT_SECRET '{secret}',
              OAUTH2_SERVER_URI '{POLARIS_URL}/api/catalog/v1/oauth/tokens', OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL')"""
    )
    con.execute(
        f"""CREATE OR REPLACE SECRET s3_secret (
              TYPE S3, KEY_ID '{S3_KEY}', SECRET '{S3_SECRET}', ENDPOINT '{host}',
              URL_STYLE 'path', USE_SSL false, REGION '{S3_REGION}')"""
    )
    con.execute(
        f"""ATTACH '{CATALOG}' AS lake (TYPE ICEBERG, ENDPOINT '{POLARIS_URL}/api/catalog',
              SECRET polaris_secret, ACCESS_DELEGATION_MODE 'none')"""
    )
    return con


def main() -> None:
    con = connect()
    print("HEDIS-style rates (overall):")
    print(con.sql("SELECT measure, denominator, numerator, rate FROM lake.gold.hedis_rates WHERE state = 'ALL' ORDER BY measure").df().to_string(index=False))
    print("\nEncounters per month (partition-friendly filter on a DATE column):")
    print(
        con.sql(
            """SELECT date_trunc('month', contact_date) AS month, count(*) AS encounters
               FROM lake.gold.fact_encounter GROUP BY 1 ORDER BY 1 DESC LIMIT 6"""
        ).df().to_string(index=False)
    )
    print("\nAverage systolic BP by state (join fact + dim, all inside DuckDB):")
    print(
        con.sql(
            """SELECT d.state, count(*) AS visits, round(avg(f.bp_systolic), 1) AS avg_sys
               FROM lake.gold.fact_encounter f JOIN lake.gold.dim_patient d USING (pat_key)
               WHERE f.bp_systolic IS NOT NULL GROUP BY d.state ORDER BY d.state"""
        ).df().to_string(index=False)
    )


if __name__ == "__main__":
    main()
