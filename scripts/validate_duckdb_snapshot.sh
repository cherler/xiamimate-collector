#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/load_collector_env.sh
source "$ROOT_DIR/scripts/load_collector_env.sh"

PYTHON_BIN="${XIAMIMATE_PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
SNAPSHOT_DB_PATH="${1:-}"

if [[ -z "$SNAPSHOT_DB_PATH" ]]; then
    echo "usage: bash scripts/validate_duckdb_snapshot.sh <snapshot-db-path>" >&2
    exit 1
fi

if [[ ! -f "$SNAPSHOT_DB_PATH" ]]; then
    echo "DuckDB snapshot not found: $SNAPSHOT_DB_PATH" >&2
    exit 1
fi

if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "python runtime not found: $PYTHON_BIN" >&2
    exit 1
fi

SNAPSHOT_DB_PATH="$SNAPSHOT_DB_PATH" "$PYTHON_BIN" - <<'PY'
import os
import sys

import duckdb

snapshot_path = os.environ["SNAPSHOT_DB_PATH"]
try:
    conn = duckdb.connect(snapshot_path, read_only=True)
    try:
        table_count = conn.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        """).fetchone()[0]
    finally:
        conn.close()
except Exception as exc:
    print(f"DuckDB snapshot open failed: {snapshot_path}: {exc}", file=sys.stderr)
    raise SystemExit(1)

if table_count <= 0:
    print(f"DuckDB snapshot has no user tables: {snapshot_path}", file=sys.stderr)
    raise SystemExit(1)

print(f"DuckDB snapshot OK: {snapshot_path} tables={table_count}")
PY