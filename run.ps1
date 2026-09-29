# PowerShell task runner for IceLake Health
param(
    [Parameter(Position=0)]
    [string]$Command = "help",
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$RemainingArgs
)

$DC = "docker compose --env-file .env -f docker/docker-compose.yml"

function Run-Pyx($script, $argsList) {
    $argString = ""
    if ($argsList) {
        $argString = $argsList -join " "
    }
    Invoke-Expression "$DC exec -T jupyter python $script $argString"
}

switch ($Command) {
    "init" {
        python -c @"
import secrets, re
from pathlib import Path

env_file = Path('.env')
example_file = Path('.env.example')
if not env_file.exists():
    env_file.write_text(example_file.read_text())

content = env_file.read_text()
def gen(n): return secrets.token_hex(n)

keys = {
    'GARAGE_DEFAULT_ACCESS_KEY': 'GK' + gen(12),
    'GARAGE_DEFAULT_SECRET_KEY': gen(32),
    'GARAGE_RPC_SECRET': gen(32),
    'GARAGE_ADMIN_TOKEN': gen(24),
    'POLARIS_ROOT_CLIENT_SECRET': gen(16),
    'PHI_HASH_SALT': gen(16),
}

for k, v in keys.items():
    content = re.sub(rf'^{k}=$', f'{k}={v}', content, flags=re.M)

env_file.write_text(content)

rpc = re.search(r'^GARAGE_RPC_SECRET=(.*)$', content, re.M).group(1)
admin = re.search(r'^GARAGE_ADMIN_TOKEN=(.*)$', content, re.M).group(1)
region = re.search(r'^S3_REGION=(.*)$', content, re.M).group(1)

toml = f'''metadata_dir = \"/var/lib/garage/meta\"
data_dir = \"/var/lib/garage/data\"
db_engine = \"sqlite\"
replication_factor = 1

rpc_bind_addr = \"[::]:3901\"
rpc_public_addr = \"127.0.0.1:3901\"
rpc_secret = \"{rpc}\"

[s3_api]
s3_region = \"{region}\"
api_bind_addr = \"[::]:3900\"

[admin]
api_bind_addr = \"[::]:3903\"
admin_token = \"{admin}\"
'''
Path('docker/garage/garage.toml').write_text(toml)
print('OK: .env and docker/garage/garage.toml created.')
"@
    }
    "up" {
        Invoke-Expression "$DC up -d --build garage polaris jupyter"
    }
    "up-persist" {
        Invoke-Expression "$DC -f docker/docker-compose.persist.yml up -d --build garage postgres polaris-bootstrap polaris jupyter"
    }
    "catalog" {
        Run-Pyx "src/catalog/polaris_setup.py" $RemainingArgs
    }
    "trino" {
        Invoke-Expression "$DC up -d --force-recreate trino"
    }
    "smoke" {
        Run-Pyx "src/common/smoke_test.py" $RemainingArgs
    }
    "gen" {
        Run-Pyx "src/ingestion/generate_clarity_data.py" $RemainingArgs
    }
    "bronze" {
        Run-Pyx "src/ingestion/load_bronze.py" $RemainingArgs
    }
    "silver" {
        Run-Pyx "src/transform/build_silver.py" $RemainingArgs
    }
    "dq" {
        Run-Pyx "src/quality/run_checks.py" $RemainingArgs
    }
    "gold" {
        Run-Pyx "src/transform/build_gold.py" $RemainingArgs
    }
    "pipeline" {
        if (-not $RemainingArgs -or $RemainingArgs.Count -eq 0) {
            Write-Error "Usage: .\run.ps1 pipeline <batch 0-3>"
            return
        }
        $b = $RemainingArgs[0]
        & $PSCommandPath bronze --batches $b
        & $PSCommandPath silver --batches $b
        & $PSCommandPath dq
        & $PSCommandPath gold
    }
    "evolve" {
        Run-Pyx "src/transform/schema_evolution_demo.py" $RemainingArgs
    }
    "maintain" {
        Run-Pyx "src/maintenance/table_ops.py" $RemainingArgs
    }
    "timetravel" {
        Run-Pyx "src/query/time_travel_demo.py" $RemainingArgs
    }
    "duckdb" {
        Run-Pyx "src/query/duckdb_queries.py" $RemainingArgs
    }
    "trino-sql" {
        Run-Pyx "src/query/trino_queries.py" $RemainingArgs
    }
    "bench" {
        Run-Pyx "src/benchmark/run_benchmark.py" $RemainingArgs
    }
    "rbac" {
        Run-Pyx "src/catalog/polaris_rbac.py" $RemainingArgs
    }
    "reregister" {
        Run-Pyx "src/catalog/reregister_tables.py" $RemainingArgs
    }
    "s3ls" {
        Run-Pyx "src/common/s3_ls.py" $RemainingArgs
    }
    "trino-cli" {
        Invoke-Expression "$DC exec trino trino --catalog iceberg --schema gold"
    }
    "shell" {
        Invoke-Expression "$DC exec -it jupyter bash"
    }
    "logs" {
        $argString = if ($RemainingArgs) { $RemainingArgs -join " " } else { "" }
        Invoke-Expression "$DC logs -f --tail=100 $argString"
    }
    "ps" {
        Invoke-Expression "$DC ps"
    }
    "down" {
        Invoke-Expression "$DC down"
    }
    "nuke" {
        $confirm = Read-Host "This deletes ALL lakehouse data (volumes). Type 'yes' to proceed"
        if ($confirm -eq "yes") {
            Invoke-Expression "$DC down -v"
        }
    }
    Default {
        Write-Host @"
Setup    : init | up | smoke | catalog | trino | smoke --spark
Data     : gen [--patients N --clean] | bronze --batches 0 | silver --batches 0 | dq | gold | pipeline <0-3>
Demos    : evolve | maintain | timetravel | duckdb | trino-sql [--rbac] | bench | rbac
Inspect  : s3ls [prefix] | trino-cli | shell | logs [service] | ps
Recovery : reregister   (Polaris restarted and forgot the tables? files in S3 are safe)
Teardown : down | nuke
"@
    }
}
