#!/usr/bin/env python
"""Teardown helper: purge all tables, drop namespaces, delete the catalog. (Use ./run.sh nuke to wipe everything instead.)"""
import requests

from catalog.polaris_setup import CAT, MGMT, hdr, token
from common.config import CATALOG, env
from common.spark import get_spark

spark = get_spark("teardown")
for ns in ("bronze", "silver", "gold"):
    try:
        for row in spark.sql(f"SHOW TABLES IN lake.{ns}").collect():
            print(f"drop {ns}.{row.tableName}")
            spark.sql(f"DROP TABLE lake.{ns}.{row.tableName} PURGE")
    except Exception as e:  # noqa: BLE001
        print(f"skip {ns}: {e}")

root = token(env("POLARIS_ROOT_CLIENT_ID", "root"), env("POLARIS_ROOT_CLIENT_SECRET", required=True))
for ns in ("bronze", "silver", "gold"):
    requests.delete(f"{CAT}/{CATALOG}/namespaces/{ns}", headers=hdr(root), timeout=30)
r = requests.delete(f"{MGMT}/catalogs/{CATALOG}", headers=hdr(root), timeout=30)
print("delete catalog ->", r.status_code)
