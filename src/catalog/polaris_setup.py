#!/usr/bin/env python
"""Bootstrap Polaris (idempotent - safe to re-run):
   catalog `health` on Garage S3, namespaces bronze/silver/gold,
   principals `engineer` (full access) and `analyst` (locked down later by polaris_rbac.py).
Writes the generated client credentials back into .env."""
from __future__ import annotations

import sys
import time

import requests

from common.config import CATALOG, POLARIS_URL, S3_BUCKET, S3_ENDPOINT, env, update_env

CAT = f"{POLARIS_URL}/api/catalog/v1"
MGMT = f"{POLARIS_URL}/api/management/v1"


def token(client_id: str, client_secret: str) -> str:
    r = requests.post(
        f"{CAT}/oauth/tokens",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def hdr(t: str) -> dict:
    return {"Authorization": f"Bearer {t}", "Content-Type": "application/json"}


def wait_for_root() -> str:
    last = None
    for _ in range(60):
        try:
            return token(env("POLARIS_ROOT_CLIENT_ID", "root"), env("POLARIS_ROOT_CLIENT_SECRET", required=True))
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2)
    sys.exit(f"Polaris not reachable / root credentials rejected: {last}\nTry: ./run.sh logs polaris")


def post(url: str, t: str, *bodies: dict, label: str):
    """POST trying each body shape in order (Polaris versions differ: flat vs wrapped JSON)."""
    last = None
    for body in bodies:
        r = requests.post(url, headers=hdr(t), json=body, timeout=30)
        if r.status_code in (200, 201):
            print(f"  + {label}")
            return r
        if r.status_code == 409:
            print(f"  = {label} (already exists)")
            return r
        last = r
    sys.exit(f"FAILED {label}: HTTP {last.status_code} {last.text}")


def put(url: str, t: str, body: dict, label: str) -> None:
    r = requests.put(url, headers=hdr(t), json=body, timeout=30)
    if r.status_code not in (200, 201, 204):
        sys.exit(f"FAILED {label}: HTTP {r.status_code} {r.text}")
    print(f"  + {label}")


def ensure_principal(root: str, name: str) -> tuple[str, str]:
    key = name.upper()
    cid, sec = env(f"{key}_CLIENT_ID"), env(f"{key}_CLIENT_SECRET")
    if cid and sec:
        try:
            token(cid, sec)
            print(f"  = principal {name} (existing credentials still valid)")
            return cid, sec
        except Exception:  # noqa: BLE001
            pass
    flat = {"name": name, "type": "SERVICE"}
    r = post(f"{MGMT}/principals", root, flat, {"principal": flat}, label=f"principal {name}")
    if r.status_code == 409:  # exists but we lost the secret -> rotate by delete + create
        requests.delete(f"{MGMT}/principals/{name}", headers=hdr(root), timeout=30)
        r = post(f"{MGMT}/principals", root, flat, {"principal": flat}, label=f"principal {name} (recreated)")
    c = r.json()["credentials"]
    return c["clientId"], c["clientSecret"]


def main() -> None:
    print("1) root token")
    root = wait_for_root()

    print("2) catalog (Garage S3, static creds, no STS)")
    storage = {
        "storageType": "S3",
        "allowedLocations": [f"s3://{S3_BUCKET}/warehouse/", f"s3://{S3_BUCKET}/"],
        "endpoint": S3_ENDPOINT,
        "endpointInternal": S3_ENDPOINT,
        "stsUnavailable": True,
        "pathStyleAccess": True,
    }
    flat = {
        "name": CATALOG,
        "type": "INTERNAL",
        "properties": {
            "default-base-location": f"s3://{S3_BUCKET}/warehouse/",
            "polaris.config.drop-with-purge.enabled": "true",
        },
        "storageConfigInfo": storage,
    }
    cat_res = post(f"{MGMT}/catalogs", root, flat, {"catalog": flat}, label=f"catalog {CATALOG}")
    if cat_res.status_code == 409:
        try:
            existing = requests.get(f"{MGMT}/catalogs/{CATALOG}", headers=hdr(root)).json()
            update_body = {
                "currentEntityVersion": existing["entityVersion"],
                "properties": {
                    "default-base-location": f"s3://{S3_BUCKET}/warehouse/",
                    "polaris.config.drop-with-purge.enabled": "true",
                },
                "storageConfigInfo": storage,
            }
            requests.put(f"{MGMT}/catalogs/{CATALOG}", headers=hdr(root), json=update_body)
            print(f"  = catalog {CATALOG} storage refreshed")
        except Exception as e:
            print(f"  ! catalog update note: {e}")

    print("3) principals")
    eng_id, eng_secret = ensure_principal(root, "engineer")
    ana_id, ana_secret = ensure_principal(root, "analyst")

    print("4) roles + grants (engineer gets full content access)")
    for pr in ("data_engineer", "data_analyst"):
        post(f"{MGMT}/principal-roles", root, {"principalRole": {"name": pr}}, {"name": pr}, label=f"principal-role {pr}")
    put(f"{MGMT}/principals/engineer/principal-roles", root, {"principalRole": {"name": "data_engineer"}}, "engineer -> data_engineer")
    put(f"{MGMT}/principals/analyst/principal-roles", root, {"principalRole": {"name": "data_analyst"}}, "analyst  -> data_analyst")
    post(
        f"{MGMT}/catalogs/{CATALOG}/catalog-roles",
        root,
        {"catalogRole": {"name": "engineer_full"}},
        {"name": "engineer_full"},
        label="catalog-role engineer_full",
    )
    put(
        f"{MGMT}/catalogs/{CATALOG}/catalog-roles/engineer_full/grants",
        root,
        {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}},
        "engineer_full: CATALOG_MANAGE_CONTENT",
    )
    put(
        f"{MGMT}/principal-roles/data_engineer/catalog-roles/{CATALOG}",
        root,
        {"catalogRole": {"name": "engineer_full"}},
        "data_engineer -> engineer_full",
    )

    print("5) namespaces (medallion layers)")
    eng = token(eng_id, eng_secret)
    for ns in ("bronze", "silver", "gold"):
        post(f"{CAT}/{CATALOG}/namespaces", eng, {"namespace": [ns], "properties": {}}, label=f"namespace {ns}")

    update_env(
        {
            "ENGINEER_CLIENT_ID": eng_id,
            "ENGINEER_CLIENT_SECRET": eng_secret,
            "ENGINEER_CREDENTIAL": f"{eng_id}:{eng_secret}",
            "ANALYST_CLIENT_ID": ana_id,
            "ANALYST_CLIENT_SECRET": ana_secret,
            "ANALYST_CREDENTIAL": f"{ana_id}:{ana_secret}",
        }
    )
    print("\nDone. Credentials saved to .env. Next: ./run.sh trino")


if __name__ == "__main__":
    main()
