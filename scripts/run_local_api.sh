#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API_DIR="$ROOT_DIR/api"
ENV_FILE="$ROOT_DIR/.env"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  set -a
  source "$ENV_FILE"
  set +a
fi

POSTGRES_DB="${POSTGRES_DB:-appdb}"
POSTGRES_USER="${POSTGRES_USER:-appuser}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-appuser}"
API_PORT="${API_PORT:-8000}"
LOCAL_PG_HOST="${LOCAL_PG_HOST:-127.0.0.1}"
LOCAL_PG_PORT="${LOCAL_PG_PORT:-5432}"
SKIP_BREW_START="${SKIP_BREW_START:-0}"
AUTO_INSTALL_POSTGRES="${AUTO_INSTALL_POSTGRES:-1}"
AUTO_INSTALL_PYTHON="${AUTO_INSTALL_PYTHON:-1}"
PYTHON_BIN="${PYTHON_BIN:-}"

for bin_dir in \
  /opt/homebrew/opt/postgresql@16/bin \
  /opt/homebrew/opt/postgresql@15/bin \
  /opt/homebrew/opt/postgresql@14/bin \
  /opt/homebrew/opt/postgresql/bin \
  /opt/homebrew/opt/libpq/bin
do
  if [[ -d "$bin_dir" ]]; then
    export PATH="$bin_dir:$PATH"
  fi
done

echo "[local-api] root: $ROOT_DIR"
echo "[local-api] db: $POSTGRES_DB user=$POSTGRES_USER host=$LOCAL_PG_HOST port=$LOCAL_PG_PORT"

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[local-api] required command not found: $1" >&2
    exit 1
  fi
}

py_ok() {
  local bin="$1"
  "$bin" - <<'PY' >/dev/null 2>&1
import sys
raise SystemExit(0 if sys.version_info >= (3, 10) else 1)
PY
}

if [[ -n "$PYTHON_BIN" ]]; then
  if ! py_ok "$PYTHON_BIN"; then
    echo "[local-api] PYTHON_BIN is not Python 3.10+: $PYTHON_BIN" >&2
    exit 1
  fi
else
  for cand in \
    python3.12 \
    /opt/homebrew/bin/python3.12 \
    /opt/homebrew/opt/python@3.12/bin/python3.12 \
    python3.11 \
    /opt/homebrew/bin/python3.11 \
    python3.10 \
    /opt/homebrew/bin/python3.10 \
    python3
  do
    if command -v "$cand" >/dev/null 2>&1 && py_ok "$cand"; then
      PYTHON_BIN="$cand"
      break
    fi
  done
fi

if [[ -z "$PYTHON_BIN" ]]; then
  if [[ "$AUTO_INSTALL_PYTHON" == "1" ]] && command -v brew >/dev/null 2>&1; then
    echo "[local-api] python 3.10+ not found. installing python@3.12 via brew"
    brew install python@3.12
    for cand in /opt/homebrew/bin/python3.12 /opt/homebrew/opt/python@3.12/bin/python3.12; do
      if [[ -x "$cand" ]] && py_ok "$cand"; then
        PYTHON_BIN="$cand"
        break
      fi
    done
  fi
fi

if [[ -z "$PYTHON_BIN" ]]; then
  echo "[local-api] python 3.10+ is required (PYTHON_BIN 지정 가능)" >&2
  exit 1
fi

need_cmd "$PYTHON_BIN"

if ! command -v psql >/dev/null 2>&1 || ! command -v pg_isready >/dev/null 2>&1; then
  if [[ "$AUTO_INSTALL_POSTGRES" == "1" ]] && command -v brew >/dev/null 2>&1; then
    echo "[local-api] postgresql tools not found. installing postgresql@16 via brew"
    brew install postgresql@16
    if [[ -d "/opt/homebrew/opt/postgresql@16/bin" ]]; then
      export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"
    fi
  fi
fi

need_cmd psql
need_cmd pg_isready

if ! pg_isready -h "$LOCAL_PG_HOST" -p "$LOCAL_PG_PORT" -q; then
  if [[ "$SKIP_BREW_START" == "1" ]]; then
    echo "[local-api] postgres not ready and SKIP_BREW_START=1" >&2
    exit 1
  fi

  if command -v brew >/dev/null 2>&1; then
    for formula in postgresql@16 postgresql@15 postgresql@14 postgresql; do
      if brew list --versions "$formula" >/dev/null 2>&1; then
        echo "[local-api] starting $formula via brew services"
        brew services start "$formula" >/dev/null
        break
      fi
    done
  fi
fi

for _ in $(seq 1 30); do
  if pg_isready -h "$LOCAL_PG_HOST" -p "$LOCAL_PG_PORT" -q; then
    break
  fi
  sleep 1
done

if ! pg_isready -h "$LOCAL_PG_HOST" -p "$LOCAL_PG_PORT" -q; then
  echo "[local-api] postgres is not reachable on $LOCAL_PG_HOST:$LOCAL_PG_PORT" >&2
  exit 1
fi

echo "[local-api] ensuring role/database"
psql -h "$LOCAL_PG_HOST" -p "$LOCAL_PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
  -v app_user="$POSTGRES_USER" \
  -v app_pass="$POSTGRES_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'app_user', :'app_pass')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'app_user') \gexec
SELECT format('ALTER ROLE %I LOGIN PASSWORD %L', :'app_user', :'app_pass') \gexec
SQL

psql -h "$LOCAL_PG_HOST" -p "$LOCAL_PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
  -v app_db="$POSTGRES_DB" \
  -v app_user="$POSTGRES_USER" <<'SQL'
SELECT format('CREATE DATABASE %I OWNER %I', :'app_db', :'app_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'app_db') \gexec
SQL

export PGPASSWORD="$POSTGRES_PASSWORD"

echo "[local-api] applying migrations"
for f in \
  "$ROOT_DIR/db/001_cadastral_admin.sql" \
  "$ROOT_DIR/db/002_cadastral_label_point.sql" \
  "$ROOT_DIR/db/003_release_data_type_and_building_info.sql" \
  "$ROOT_DIR/db/004_dataset_record.sql" \
  "$ROOT_DIR/db/005_import_worker_progress.sql" \
  "$ROOT_DIR/db/006_dataset_storage_optimization.sql" \
  "$ROOT_DIR/db/007_dataset_pnu_kv.sql"
do
  psql -h "$LOCAL_PG_HOST" -p "$LOCAL_PG_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -f "$f" >/dev/null
done

PY_MM="$("$PYTHON_BIN" - <<'PY'
import sys
print(f"{sys.version_info.major}{sys.version_info.minor}")
PY
)"
VENV_DIR="$API_DIR/.venv-py$PY_MM"

if [[ ! -d "$VENV_DIR" ]]; then
  echo "[local-api] creating venv with $PYTHON_BIN -> $VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
echo "[local-api] installing dependencies"
pip install --quiet --disable-pip-version-check -r "$API_DIR/requirements.txt"

export DATABASE_URL="host=$LOCAL_PG_HOST port=$LOCAL_PG_PORT dbname=$POSTGRES_DB user=$POSTGRES_USER password=$POSTGRES_PASSWORD"
export DATA_DIR="${DATA_DIR:-$ROOT_DIR/data/uploads}"
export REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379/0}"

echo "[local-api] start uvicorn on 0.0.0.0:$API_PORT"
cd "$API_DIR"
exec uvicorn app.main:app --host 0.0.0.0 --port "$API_PORT"
