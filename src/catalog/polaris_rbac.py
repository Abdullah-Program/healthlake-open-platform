#!/usr/bin/env python
"""Phase 10 - least-privilege access.
   analyst may: list namespaces, and READ tables in namespace `gold` only.
   analyst may NOT: touch bronze/silver (they hold PHI-like columns: names, birth dates, ZIP).
Then proves it with real REST calls made as the analyst."""
from __future__ import annotations

import requests

from catalog.polaris_setup import CAT, MGMT, hdr, post, put, token
from common.config import CATALOG, creds, env

GOLD_PRIVS = ["TABLE_LIST", "NAMESPACE_READ_PROPERTIES", "TABLE_READ_PROPERTIES", "TABLE_READ_DATA"]


def main() -> None:
    root = token(env("POLARIS_ROOT_CLIENT_ID", "root"), env("POLARIS_ROOT_CLIENT_SECRET", required=True))

    post(
        f"{MGMT}/catalogs/{CATALOG}/catalog-roles",
        root,
        {"catalogRole": {"name": "analyst_gold_read"}},
        {"name": "analyst_gold_read"},
        label="catalog-role analyst_gold_read",
    )
    grants_url = f"{MGMT}/catalogs/{CATALOG}/catalog-roles/analyst_gold_read/grants"
    put(grants_url, root, {"grant": {"type": "catalog", "privilege": "NAMESPACE_LIST"}}, "catalog-level: NAMESPACE_LIST")
    for priv in GOLD_PRIVS:
        put(grants_url, root, {"grant": {"type": "namespace", "namespace": ["gold"], "privilege": priv}}, f"namespace gold: {priv}")
    put(
        f"{MGMT}/principal-roles/data_analyst/catalog-roles/{CATALOG}",
        root,
        {"catalogRole": {"name": "analyst_gold_read"}},
        "data_analyst -> analyst_gold_read",
    )

    print("\nVerifying as `analyst` ...")
    cid, sec = creds("analyst")
    t = token(cid, sec)
    checks = [
        ("silver.patient  (expect 403 Forbidden)", "silver", "patient"),
        ("bronze.patient  (expect 403 Forbidden)", "bronze", "patient"),
        ("gold.hedis_rates (expect 200 OK, or 404 if gold not built yet)", "gold", "hedis_rates"),
    ]
    for label, ns, table in checks:
        r = requests.get(f"{CAT}/{CATALOG}/namespaces/{ns}/tables/{table}", headers=hdr(t), timeout=30)
        print(f"  {label:<62} -> HTTP {r.status_code}")
    print("\nNow try the same from Trino:  ./run.sh trino-sql --rbac")


if __name__ == "__main__":
    main()
