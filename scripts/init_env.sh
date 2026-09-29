#!/usr/bin/env bash
# Creates .env with fresh random secrets (per machine, never committed) and renders garage.toml
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] || cp .env.example .env
gen() { openssl rand -hex "$1"; }
fill() { # fill KEY VALUE  -> only if KEY= is still empty
  if grep -qE "^$1=$" .env; then sed -i "s|^$1=$|$1=$2|" .env; fi
}
fill GARAGE_DEFAULT_ACCESS_KEY "GK$(gen 12)"   # Garage key ids look like GK + 24 hex chars
fill GARAGE_DEFAULT_SECRET_KEY "$(gen 32)"
fill GARAGE_RPC_SECRET "$(gen 32)"
fill GARAGE_ADMIN_TOKEN "$(gen 24)"
fill POLARIS_ROOT_CLIENT_SECRET "$(gen 16)"
fill PHI_HASH_SALT "$(gen 16)"

RPC=$(grep '^GARAGE_RPC_SECRET=' .env | cut -d= -f2)
ADMIN=$(grep '^GARAGE_ADMIN_TOKEN=' .env | cut -d= -f2)
REGION=$(grep '^S3_REGION=' .env | cut -d= -f2)

mkdir -p docker/garage
cat > docker/garage/garage.toml <<EOF
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
db_engine = "sqlite"
replication_factor = 1

rpc_bind_addr = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "${RPC}"

[s3_api]
s3_region = "${REGION}"
api_bind_addr = "[::]:3900"

[admin]
api_bind_addr = "[::]:3903"
admin_token = "${ADMIN}"
EOF

echo "OK: .env and docker/garage/garage.toml created (both are gitignored)."
echo "Next: ./run.sh up"
