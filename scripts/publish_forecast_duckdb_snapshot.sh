#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/load_collector_env.sh
source "$ROOT_DIR/scripts/load_collector_env.sh"

PYTHON_BIN="${XIAMIMATE_PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
ROLLING_CURRENT_LINK="${XIAMIMATE_DUCKDB_SNAPSHOT_CURRENT_LINK:-/data/xiamimate/duckdb/snapshots/current}"
ROLLING_SNAPSHOT_DB_PATH="$ROLLING_CURRENT_LINK/local_analytics.duckdb"
ROLLING_MANIFEST_PATH="$ROLLING_CURRENT_LINK/snapshot_manifest.json"
FORECAST_BASE_DIR="${FORECAST_DUCKDB_SNAPSHOT_BASE_DIR:-/data/xiamimate/duckdb/forecast_snapshots}"
FORECAST_RUNS_DIR="${FORECAST_DUCKDB_SNAPSHOT_RUNS_DIR:-$FORECAST_BASE_DIR/runs}"
FORECAST_CURRENT_LINK="${FORECAST_DUCKDB_SNAPSHOT_CURRENT_LINK:-$FORECAST_BASE_DIR/current}"
FORECAST_RETENTION_COUNT="${FORECAST_DUCKDB_SNAPSHOT_RETENTION_COUNT:-1}"
FORECAST_COPY_MODE="${FORECAST_DUCKDB_SNAPSHOT_COPY_MODE:-auto}"
FORECAST_LOCK_FILE="${FORECAST_DUCKDB_SNAPSHOT_LOCK_FILE:-${XIAMIMATE_LOG_DIR:-$FORECAST_BASE_DIR}/publish_forecast_duckdb_snapshot.lock}"
ROLLING_LOCK_FILE="${XIAMIMATE_DUCKDB_SNAPSHOT_LOCK_FILE:-${XIAMIMATE_LOG_DIR:-/data/xiamimate/collector/logs}/publish_duckdb_snapshot.lock}"
REPORT_SCRIPT="$ROOT_DIR/scripts/report_forecast_snapshot_ready.py"

if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "python runtime not found: $PYTHON_BIN" >&2
    exit 1
fi

if [[ ! "$FORECAST_RETENTION_COUNT" =~ ^[0-9]+$ ]] || [[ "$FORECAST_RETENTION_COUNT" -lt 1 ]]; then
    echo "FORECAST_DUCKDB_SNAPSHOT_RETENTION_COUNT must be >= 1" >&2
    exit 1
fi

mkdir -p "$FORECAST_RUNS_DIR" "$(dirname "$FORECAST_LOCK_FILE")" "$(dirname "$ROLLING_LOCK_FILE")"

exec 8>"$ROLLING_LOCK_FILE"
flock -x 8

exec 9>"$FORECAST_LOCK_FILE"
flock -x 9

if [[ ! -f "$ROLLING_SNAPSHOT_DB_PATH" ]]; then
    echo "rolling current DuckDB snapshot missing: $ROLLING_SNAPSHOT_DB_PATH" >&2
    exit 1
fi

SNAPSHOT_ID="${FORECAST_SNAPSHOT_ID:-forecast_$(date -u +%Y%m%dT%H%M%SZ)}"
SNAPSHOT_DIR="$FORECAST_RUNS_DIR/$SNAPSHOT_ID"
SNAPSHOT_DB_PATH="$SNAPSHOT_DIR/local_analytics.duckdb"
SNAPSHOT_MANIFEST_PATH="$SNAPSHOT_DIR/snapshot_manifest.json"
SOURCE_ROLLING_DIR="$(cd "$(dirname "$ROLLING_SNAPSHOT_DB_PATH")" && pwd)"
SOURCE_ROLLING_ID="$(basename "$SOURCE_ROLLING_DIR")"

if [[ -e "$SNAPSHOT_DIR" ]]; then
    echo "forecast snapshot dir already exists: $SNAPSHOT_DIR" >&2
    exit 1
fi

mkdir -p "$SNAPSHOT_DIR"

copy_file() {
    local source_path="$1"
    local target_path="$2"
    case "$FORECAST_COPY_MODE" in
        hardlink)
            ln "$source_path" "$target_path"
            ;;
        copy)
            cp -p "$source_path" "$target_path"
            ;;
        auto)
            ln "$source_path" "$target_path" 2>/dev/null || cp -p "$source_path" "$target_path"
            ;;
        *)
            echo "unsupported FORECAST_DUCKDB_SNAPSHOT_COPY_MODE=$FORECAST_COPY_MODE (expected auto|hardlink|copy)" >&2
            exit 1
            ;;
    esac
}

copy_file "$ROLLING_SNAPSHOT_DB_PATH" "$SNAPSHOT_DB_PATH"
if [[ -f "$ROLLING_MANIFEST_PATH" ]]; then
    copy_file "$ROLLING_MANIFEST_PATH" "$SNAPSHOT_MANIFEST_PATH.source"
fi

export SNAPSHOT_ID SNAPSHOT_DIR SNAPSHOT_DB_PATH SNAPSHOT_MANIFEST_PATH ROLLING_SNAPSHOT_DB_PATH ROLLING_MANIFEST_PATH SOURCE_ROLLING_ID FORECAST_COPY_MODE
"$PYTHON_BIN" - <<'PY'
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb

snapshot_db = Path(os.environ["SNAPSHOT_DB_PATH"])
manifest_path = Path(os.environ["SNAPSHOT_MANIFEST_PATH"])
rolling_db = Path(os.environ["ROLLING_SNAPSHOT_DB_PATH"])
rolling_manifest = Path(os.environ["ROLLING_MANIFEST_PATH"])

conn = duckdb.connect(str(snapshot_db), read_only=True)
try:
    table_count = int(conn.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    """).fetchone()[0])
finally:
    conn.close()

source_manifest = None
source_manifest_copy = manifest_path.with_suffix(".json.source")
if source_manifest_copy.exists():
    try:
        source_manifest = json.loads(source_manifest_copy.read_text(encoding="utf-8"))
    except Exception:
        source_manifest = None

manifest = {
    "snapshot_id": os.environ["SNAPSHOT_ID"],
    "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "source_system": "ecs2-collector",
    "source_rolling_snapshot_id": os.environ.get("SOURCE_ROLLING_ID"),
    "source_rolling_snapshot_db": str(rolling_db),
    "source_rolling_manifest": str(rolling_manifest),
    "source_rolling_manifest_payload": source_manifest,
    "copy_mode": os.environ.get("FORECAST_COPY_MODE"),
    "snapshot_dir": os.environ["SNAPSHOT_DIR"],
    "snapshot_db": str(snapshot_db),
    "snapshot_size_bytes": snapshot_db.stat().st_size,
    "snapshot_mtime_utc": datetime.fromtimestamp(snapshot_db.stat().st_mtime, tz=timezone.utc).isoformat(),
    "table_count": table_count,
    "validation": {
        "open_read_only": True,
        "table_count_gt_zero": table_count > 0,
    },
}
manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
PY

ln -sfn "$SNAPSHOT_DIR" "$FORECAST_CURRENT_LINK"

snapshot_size_bytes="$(stat -c '%s' "$SNAPSHOT_DB_PATH" 2>/dev/null || stat -f '%z' "$SNAPSHOT_DB_PATH")"
source_mtime_epoch="$(stat -c '%Y' "$SNAPSHOT_DB_PATH" 2>/dev/null || stat -f '%m' "$SNAPSHOT_DB_PATH")"
source_mtime_utc="$(date -u -d "@$source_mtime_epoch" +%FT%TZ 2>/dev/null || date -u -r "$source_mtime_epoch" +%FT%TZ)"

"$PYTHON_BIN" "$REPORT_SCRIPT" \
    --snapshot-id "$SNAPSHOT_ID" \
    --duckdb-path "$SNAPSHOT_DB_PATH" \
    --manifest-path "$SNAPSHOT_MANIFEST_PATH" \
    --snapshot-size-bytes "$snapshot_size_bytes" \
    --source-db-mtime-utc "$source_mtime_utc" \
    --status ready

if [[ "$FORECAST_RETENTION_COUNT" -le 1 ]]; then
    mapfile -t old_snapshots < <(find "$FORECAST_RUNS_DIR" -mindepth 1 -maxdepth 1 -type d | grep -vxF "$SNAPSHOT_DIR" || true)
else
    mapfile -t old_snapshots < <(find "$FORECAST_RUNS_DIR" -mindepth 1 -maxdepth 1 -type d | sort -r | grep -vxF "$SNAPSHOT_DIR" | tail -n +$((FORECAST_RETENTION_COUNT)) || true)
fi
for old_snapshot in "${old_snapshots[@]:-}"; do
    [[ -z "$old_snapshot" ]] && continue
    rm -rf "$old_snapshot"
done

echo "forecast_snapshot_id=$SNAPSHOT_ID"
echo "forecast_snapshot_dir=$SNAPSHOT_DIR"
echo "forecast_current_link=$FORECAST_CURRENT_LINK"
echo "source_rolling_snapshot_id=$SOURCE_ROLLING_ID"
echo "forecast_retention_count=$FORECAST_RETENTION_COUNT"
ls -lah "$SNAPSHOT_DIR"