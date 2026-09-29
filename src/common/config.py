"""Shared config. Reads .env (project root) every time a script starts."""
from __future__ import annotations

import os
import re
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parents[2]))
ENV_FILE = ROOT / ".env"
load_dotenv(ENV_FILE, override=True)


def env(key: str, default: str | None = None, required: bool = False) -> str | None:
    val = os.getenv(key, default)
    if required and not val:
        raise SystemExit(f"Missing {key} in {ENV_FILE}. Did you run ./run.sh init and ./run.sh catalog?")
    return val or default


def update_env(updates: dict[str, str]) -> None:
    """Write/replace KEY=VALUE lines in .env (keeps everything else)."""
    lines = ENV_FILE.read_text().splitlines() if ENV_FILE.exists() else []
    for key, value in updates.items():
        pattern = re.compile(rf"^{re.escape(key)}=")
        for i, line in enumerate(lines):
            if pattern.match(line):
                lines[i] = f"{key}={value}"
                break
        else:
            lines.append(f"{key}={value}")
        os.environ[key] = value
    ENV_FILE.write_text("\n".join(lines) + "\n")


POLARIS_URL = env("POLARIS_URL", "http://polaris:8181")
CATALOG = env("POLARIS_CATALOG", "health")
S3_ENDPOINT = env("S3_ENDPOINT_INTERNAL", "http://garage:3900")
S3_REGION = env("S3_REGION", "us-east-1")
S3_BUCKET = env("S3_BUCKET", "healthlake")
S3_KEY = env("GARAGE_DEFAULT_ACCESS_KEY")
S3_SECRET = env("GARAGE_DEFAULT_SECRET_KEY")
SALT = env("PHI_HASH_SALT", "dev-salt")
MEASUREMENT_YEAR = int(env("MEASUREMENT_YEAR", "2025"))


def creds(role: str) -> tuple[str, str]:
    """role = 'engineer' | 'analyst' -> (client_id, client_secret)"""
    cid = env(f"{role.upper()}_CLIENT_ID")
    sec = env(f"{role.upper()}_CLIENT_SECRET")
    if not cid or not sec:
        raise SystemExit(f"No credentials for '{role}'. Run: ./run.sh catalog")
    return cid, sec
