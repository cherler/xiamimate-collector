#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/load_collector_env.sh
source "$ROOT_DIR/scripts/load_collector_env.sh"

PYTHON_BIN="${XIAMIMATE_PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
SOURCE_DB="${XIAMIMATE_DUCKDB_PATH:-}"
SNAPSHOT_BASE_DIR="${XIAMIMATE_DUCKDB_SNAPSHOT_BASE_DIR:-/data/xiamimate/duckdb/snapshots}"
SNAPSHOT_RUNS_DIR="${XIAMIMATE_DUCKDB_SNAPSHOT_RUNS_DIR:-$SNAPSHOT_BASE_DIR/runs}"
SNAPSHOT_CURRENT_LINK="${XIAMIMATE_DUCKDB_SNAPSHOT_CURRENT_LINK:-$SNAPSHOT_BASE_DIR/current}"
SNAPSHOT_RETENTION_COUNT="${XIAMIMATE_DUCKDB_SNAPSHOT_RETENTION_COUNT:-1}"
SNAPSHOT_SHA256="${XIAMIMATE_DUCKDB_SNAPSHOT_SHA256:-false}"
SNAPSHOT_COPY_WAL="${XIAMIMATE_DUCKDB_SNAPSHOT_COPY_WAL:-false}"
SNAPSHOT_LOCK_FILE="${XIAMIMATE_DUCKDB_SNAPSHOT_LOCK_FILE:-${XIAMIMATE_LOG_DIR:-$SNAPSHOT_BASE_DIR}/publish_duckdb_snapshot.lock}"
SNAPSHOT_ACCESS_LOCK_FILE="${XIAMIMATE_DUCKDB_SNAPSHOT_ACCESS_LOCK_FILE:-${XIAMIMATE_DUCKDB_ACCESS_LOCK_FILE:-}}"
SNAPSHOT_ACCESS_LOCK_TIMEOUT_SECONDS="${XIAMIMATE_DUCKDB_SNAPSHOT_ACCESS_LOCK_TIMEOUT_SECONDS:-${XIAMIMATE_DUCKDB_ACCESS_LOCK_TIMEOUT_SECONDS:-900}}"
SNAPSHOT_ACCESS_LOCK_ACQUIRED="false"

cleanup_access_lock() {
    if [[ "$SNAPSHOT_ACCESS_LOCK_ACQUIRED" == "true" && -n "$SNAPSHOT_ACCESS_LOCK_FILE" ]]; then
        : >"$SNAPSHOT_ACCESS_LOCK_FILE" || true
        flock -u 8 >/dev/null 2>&1 || true
    fi
}

if [[ -z "$SOURCE_DB" ]]; then
    echo "XIAMIMATE_DUCKDB_PATH is required" >&2
    exit 1
fi

if [[ ! -f "$SOURCE_DB" ]]; then
    echo "live DuckDB not found: $SOURCE_DB" >&2
    exit 1
fi

if [[ ! "$SNAPSHOT_RETENTION_COUNT" =~ ^[0-9]+$ ]] || [[ "$SNAPSHOT_RETENTION_COUNT" -lt 1 ]]; then
    echo "XIAMIMATE_DUCKDB_SNAPSHOT_RETENTION_COUNT must be >= 1" >&2
    exit 1
fi

if [[ ! "$SNAPSHOT_ACCESS_LOCK_TIMEOUT_SECONDS" =~ ^[0-9]+$ ]]; then
    echo "XIAMIMATE_DUCKDB_SNAPSHOT_ACCESS_LOCK_TIMEOUT_SECONDS must be a non-negative integer" >&2
    exit 1
fi

if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "python runtime not found: $PYTHON_BIN" >&2
    exit 1
fi

mkdir -p "$SNAPSHOT_RUNS_DIR" "$(dirname "$SNAPSHOT_LOCK_FILE")"

exec 9>"$SNAPSHOT_LOCK_FILE"
flock -x 9

if [[ -n "$SNAPSHOT_ACCESS_LOCK_FILE" ]]; then
    mkdir -p "$(dirname "$SNAPSHOT_ACCESS_LOCK_FILE")"
    # Open append-mode so that the current holder's metadata stays readable while we wait.
    exec 8>>"$SNAPSHOT_ACCESS_LOCK_FILE"
    if ! flock -x -w "$SNAPSHOT_ACCESS_LOCK_TIMEOUT_SECONDS" 8; then
        echo "timed out waiting for DuckDB access lock: $SNAPSHOT_ACCESS_LOCK_FILE" >&2
        exit 1
    fi
    SNAPSHOT_ACCESS_LOCK_ACQUIRED="true"
    trap cleanup_access_lock EXIT
    printf 'pid=%s\nrole=duckdb_snapshot_publish\nacquired_at=%s\n' "$$" "$(date -u +%FT%TZ)" >"$SNAPSHOT_ACCESS_LOCK_FILE"
fi

SNAPSHOT_ID="$(date -u +%Y%m%dT%H%M%SZ)"
SNAPSHOT_DIR="$SNAPSHOT_RUNS_DIR/$SNAPSHOT_ID"
SNAPSHOT_DB_PATH="$SNAPSHOT_DIR/local_analytics.duckdb"
SNAPSHOT_MANIFEST_PATH="$SNAPSHOT_DIR/snapshot_manifest.json"

export SOURCE_DB SNAPSHOT_ID SNAPSHOT_DIR SNAPSHOT_DB_PATH SNAPSHOT_MANIFEST_PATH SNAPSHOT_SHA256 SNAPSHOT_COPY_WAL

"$PYTHON_BIN" - <<'PY'
import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb


def sql_quote(value: str) -> str:
    return value.replace("'", "''")


source_db = Path(os.environ["SOURCE_DB"]).expanduser().resolve()
snapshot_id = os.environ["SNAPSHOT_ID"]
snapshot_dir = Path(os.environ["SNAPSHOT_DIR"]).expanduser().resolve()
snapshot_db = Path(os.environ["SNAPSHOT_DB_PATH"]).expanduser().resolve()
manifest_path = Path(os.environ["SNAPSHOT_MANIFEST_PATH"]).expanduser().resolve()
calculate_sha256 = os.environ.get("SNAPSHOT_SHA256", "false").strip().lower() in {"1", "true", "yes", "on"}
copy_wal = os.environ.get("SNAPSHOT_COPY_WAL", "false").strip().lower() in {"1", "true", "yes", "on"}

try:
    snapshot_dir.mkdir(parents=True, exist_ok=False)

    shutil.copy2(source_db, snapshot_db)
    source_wal = Path(f"{source_db}.wal")
    snapshot_wal = Path(f"{snapshot_db}.wal")
    wal_copied = False
    if copy_wal and source_wal.exists():
        shutil.copy2(source_wal, snapshot_wal)
        wal_copied = True

    validate_conn = duckdb.connect(str(snapshot_db), read_only=True)
    try:
        table_count = int(validate_conn.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        """).fetchone()[0])
    finally:
        validate_conn.close()

    snapshot_sha256 = None
    if calculate_sha256:
        sha256 = hashlib.sha256()
        with snapshot_db.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                sha256.update(chunk)
        snapshot_sha256 = sha256.hexdigest()

    manifest = {
        "snapshot_id": snapshot_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_db": str(source_db),
        "source_db_size_bytes": source_db.stat().st_size,
        "source_db_mtime_utc": datetime.fromtimestamp(source_db.stat().st_mtime, tz=timezone.utc).isoformat(),
        "source_wal": str(source_wal),
        "source_wal_copy_requested": copy_wal,
        "source_wal_copied": wal_copied,
        "source_wal_size_bytes": source_wal.stat().st_size if source_wal.exists() else 0,
        "snapshot_db": str(snapshot_db),
        "snapshot_size_bytes": snapshot_db.stat().st_size,
        "snapshot_wal": str(snapshot_wal),
        "snapshot_wal_size_bytes": snapshot_wal.stat().st_size if snapshot_wal.exists() else 0,
        "snapshot_sha256": snapshot_sha256,
        "snapshot_sha256_calculated": calculate_sha256,
        "table_count": table_count,
        "validation": {
            "open_read_only": True,
            "table_count_gt_zero": table_count > 0,
        },
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
except Exception:
    shutil.rmtree(snapshot_dir, ignore_errors=True)
    raise
PY

ln -sfn "$SNAPSHOT_DIR" "$SNAPSHOT_CURRENT_LINK"

mapfile -t old_snapshots < <(find "$SNAPSHOT_RUNS_DIR" -mindepth 1 -maxdepth 1 -type d | sort -r | grep -vxF "$SNAPSHOT_DIR" | tail -n +$((SNAPSHOT_RETENTION_COUNT + 1)) || true)
for old_snapshot in "${old_snapshots[@]:-}"; do
    [[ -z "$old_snapshot" ]] && continue
    rm -rf "$old_snapshot"
done

echo "snapshot_id=$SNAPSHOT_ID"
echo "snapshot_dir=$SNAPSHOT_DIR"
echo "current_link=$SNAPSHOT_CURRENT_LINK"
echo "retention_count=$SNAPSHOT_RETENTION_COUNT"
ls -lah "$SNAPSHOT_DIR"
