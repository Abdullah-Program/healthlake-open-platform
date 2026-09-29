#!/usr/bin/env bash
# Tiny task runner. ./run.sh help
set -euo pipefail
cd "$(dirname "$0")"
SELF="$PWD/run.sh"

DC="docker compose --env-file .env -f docker/docker-compose.yml"
PYX="$DC exec -T jupyter python"

cmd="${1:-help}"
shift || true

case "$cmd" in
  init)        bash scripts/init_env.sh ;;
  up)          $DC up -d --build garage polaris jupyter ;;
  up-persist)  $DC -f docker/docker-compose.persist.yml up -d --build garage postgres polaris-bootstrap polaris jupyter ;;
  catalog)     $PYX src/catalog/polaris_setup.py ;;
  trino)       $DC up -d --force-recreate trino ;;
  smoke)       $PYX src/common/smoke_test.py "$@" ;;
  gen)         $PYX src/ingestion/generate_clarity_data.py "$@" ;;
  bronze)      $PYX src/ingestion/load_bronze.py "$@" ;;
  silver)      $PYX src/transform/build_silver.py "$@" ;;
  dq)          $PYX src/quality/run_checks.py "$@" ;;
  gold)        $PYX src/transform/build_gold.py "$@" ;;
  pipeline)    b="${1:?usage: ./run.sh pipeline <batch 0-3>}"
               "$SELF" bronze --batches "$b"; "$SELF" silver --batches "$b"; "$SELF" dq; "$SELF" gold ;;
  evolve)      $PYX src/transform/schema_evolution_demo.py ;;
  maintain)    $PYX src/maintenance/table_ops.py "$@" ;;
  timetravel)  $PYX src/query/time_travel_demo.py ;;
  duckdb)      $PYX src/query/duckdb_queries.py ;;
  trino-sql)   $PYX src/query/trino_queries.py "$@" ;;
  bench)       $PYX src/benchmark/run_benchmark.py "$@" ;;
  rbac)        $PYX src/catalog/polaris_rbac.py ;;
  reregister)  $PYX src/catalog/reregister_tables.py ;;
  s3ls)        $PYX src/common/s3_ls.py "$@" ;;
  trino-cli)   $DC exec trino trino --catalog iceberg --schema gold "$@" ;;
  shell)       $DC exec jupyter bash ;;
  logs)        $DC logs -f --tail=100 "$@" ;;
  ps)          $DC ps ;;
  down)        $DC down ;;
  nuke)        echo "This deletes ALL lakehouse data (volumes)."; read -r -p "type yes: " a
               [ "$a" = "yes" ] && $DC down -v ;;
  help|*)
    cat <<'EOF'
Setup    : init | up | smoke | catalog | trino | smoke --spark
Data     : gen [--patients N --clean] | bronze --batches 0 | silver --batches 0 | dq | gold | pipeline <0-3>
Demos    : evolve | maintain | timetravel | duckdb | trino-sql [--rbac] | bench | rbac
Inspect  : s3ls [prefix] | trino-cli | shell | logs [service] | ps
Recovery : reregister   (Polaris restarted and forgot the tables? files in S3 are safe)
Teardown : down | nuke
EOF
    ;;
esac
