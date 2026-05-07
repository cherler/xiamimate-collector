#!/usr/bin/env bash

set -euo pipefail

export PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/load_collector_env.sh
source "$ROOT_DIR/scripts/load_collector_env.sh"

TUNNEL_SCRIPT="$ROOT_DIR/scripts/manage_pg_ssh_tunnel.sh"
PYTHON_BIN="${XIAMIMATE_PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
SYNC_SCRIPT="$ROOT_DIR/data_collector/sync_duckdb_to_pg.py"
VALIDATE_SNAPSHOT_SCRIPT="$ROOT_DIR/scripts/validate_duckdb_snapshot.sh"
LOG_DIR="${XIAMIMATE_LOG_DIR:-$ROOT_DIR/logs}"
LOCK_FILE="$LOG_DIR/sync_duckdb_to_pg.lock"
DUCKDB_PATH="${PG_SYNC_DUCKDB_PATH:-${DUCKDB_PATH:-}}"
LIVE_DUCKDB_PATH="${XIAMIMATE_DUCKDB_PATH:-}"
DUCKDB_SOURCE="${PG_SYNC_DUCKDB_SOURCE:-live}"
SNAPSHOT_CURRENT_LINK="${XIAMIMATE_DUCKDB_SNAPSHOT_CURRENT_LINK:-/data/xiamimate/duckdb/snapshots/current}"
SNAPSHOT_DB_PATH="$SNAPSHOT_CURRENT_LINK/local_analytics.duckdb"
REFRESH_SNAPSHOT="${PG_SYNC_REFRESH_SNAPSHOT:-false}"
INCLUDE_HISTORY="${PG_SYNC_INCLUDE_HISTORY:-true}"
DUCKDB_ACCESS_LOCK_FILE="${PG_SYNC_DUCKDB_ACCESS_LOCK_FILE:-${XIAMIMATE_DUCKDB_ACCESS_LOCK_FILE:-}}"
DUCKDB_ACCESS_LOCK_TIMEOUT_SECONDS="${PG_SYNC_DUCKDB_ACCESS_LOCK_TIMEOUT_SECONDS:-${XIAMIMATE_DUCKDB_ACCESS_LOCK_TIMEOUT_SECONDS:-900}}"
DUCKDB_ACCESS_LOCK_ACQUIRED="false"

mkdir -p "$LOG_DIR"

remove_csv_item() {
    local csv="$1"
    local remove_item="$2"
    local items=()
    local result=()
    local item trimmed

    IFS=',' read -r -a items <<<"$csv"
    for item in "${items[@]}"; do
        trimmed="${item#${item%%[![:space:]]*}}"
        trimmed="${trimmed%${trimmed##*[![:space:]]}}"
        if [[ -n "$trimmed" && "$trimmed" != "$remove_item" ]]; then
            result+=("$trimmed")
        fi
    done

    local IFS=','
    echo "${result[*]}"
}

cleanup_duckdb_access_lock() {
    if [[ "$DUCKDB_ACCESS_LOCK_ACQUIRED" == "true" && -n "$DUCKDB_ACCESS_LOCK_FILE" ]]; then
        : >"$DUCKDB_ACCESS_LOCK_FILE" || true
        flock -u 8 >/dev/null 2>&1 || true
    fi
}

acquire_duckdb_access_lock() {
    if [[ -z "$DUCKDB_ACCESS_LOCK_FILE" ]]; then
        return 0
    fi
    if [[ ! "$DUCKDB_ACCESS_LOCK_TIMEOUT_SECONDS" =~ ^[0-9]+$ ]]; then
        echo "PG_SYNC_DUCKDB_ACCESS_LOCK_TIMEOUT_SECONDS must be a non-negative integer" >&2
        exit 1
    fi

    mkdir -p "$(dirname "$DUCKDB_ACCESS_LOCK_FILE")"
    echo "pg-sync waiting for DuckDB access lock: $DUCKDB_ACCESS_LOCK_FILE timeout=${DUCKDB_ACCESS_LOCK_TIMEOUT_SECONDS}s"
    exec 8>"$DUCKDB_ACCESS_LOCK_FILE"
    if ! flock -x -w "$DUCKDB_ACCESS_LOCK_TIMEOUT_SECONDS" 8; then
        echo "timed out waiting for DuckDB access lock: $DUCKDB_ACCESS_LOCK_FILE" >&2
        exit 1
    fi
    DUCKDB_ACCESS_LOCK_ACQUIRED="true"
    trap cleanup_all EXIT
    printf 'pid=%s\nrole=pg_sync_live_read\nacquired_at=%s\nsource=%s\n' "$$" "$(date -u +%FT%TZ)" "$DUCKDB_PATH" >"$DUCKDB_ACCESS_LOCK_FILE"
}

cleanup_tunnel() {
    if collector_pg_tunnel_enabled && [[ "${PG_SYNC_TUNNEL_KEEPALIVE:-true}" != "true" ]]; then
        PG_TUNNEL_LOCAL_HOST="${PG_SYNC_TUNNEL_LOCAL_HOST_VALUE:-${PG_TUNNEL_LOCAL_HOST:-127.0.0.1}}" \
        PG_TUNNEL_LOCAL_PORT="${PG_SYNC_TUNNEL_LOCAL_PORT_VALUE:-15433}" \
        PG_TUNNEL_PID_FILE="${PG_SYNC_TUNNEL_PID_FILE_VALUE:-$LOG_DIR/pg_sync_ssh_tunnel.pid}" \
        PG_TUNNEL_LOG_FILE="${PG_SYNC_TUNNEL_LOG_FILE_VALUE:-$LOG_DIR/pg_sync_ssh_tunnel.log}" \
        /bin/bash "$TUNNEL_SCRIPT" stop >/dev/null 2>&1 || true
    fi
}

cleanup_all() {
    cleanup_tunnel
    cleanup_duckdb_access_lock
}

collector_require_pg_env

if [[ "$INCLUDE_HISTORY" == "true" ]]; then
    PG_SYNC_SKIP_TABLES="$(remove_csv_item "${PG_SYNC_SKIP_TABLES:-}" "curated.keepa_product_history")"
    export PG_SYNC_SKIP_TABLES
fi

if [[ "$REFRESH_SNAPSHOT" == "true" ]]; then
    /bin/bash "$ROOT_DIR/scripts/publish_duckdb_snapshot.sh"
fi

if [[ -n "${PG_SYNC_DUCKDB_PATH:-}" ]]; then
    DUCKDB_PATH="$PG_SYNC_DUCKDB_PATH"
elif [[ "$DUCKDB_SOURCE" == "live" ]]; then
    if [[ -z "$LIVE_DUCKDB_PATH" ]]; then
        echo "XIAMIMATE_DUCKDB_PATH is required when PG_SYNC_DUCKDB_SOURCE=live" >&2
        exit 1
    fi
    DUCKDB_PATH="$LIVE_DUCKDB_PATH"
    acquire_duckdb_access_lock
elif [[ "$DUCKDB_SOURCE" != "snapshot" ]]; then
    echo "unsupported PG_SYNC_DUCKDB_SOURCE: $DUCKDB_SOURCE (expected live or snapshot)" >&2
    exit 1
elif [[ -f "$SNAPSHOT_DB_PATH" ]]; then
    if [[ "$REFRESH_SNAPSHOT" != "true" && "${PG_SYNC_VALIDATE_SNAPSHOT:-true}" == "true" ]]; then
        /bin/bash "$VALIDATE_SNAPSHOT_SCRIPT" "$SNAPSHOT_DB_PATH"
    fi
    DUCKDB_PATH="${PG_SYNC_DUCKDB_PATH:-$SNAPSHOT_DB_PATH}"
elif [[ "$REFRESH_SNAPSHOT" != "true" ]]; then
    echo "current DuckDB snapshot missing: $SNAPSHOT_DB_PATH" >&2
    exit 1
fi

echo "pg-sync source=$DUCKDB_SOURCE duckdb_path=$DUCKDB_PATH refresh_snapshot=$REFRESH_SNAPSHOT include_history=$INCLUDE_HISTORY skip_tables=${PG_SYNC_SKIP_TABLES:-}"

PG_SYNC_TUNNEL_LOCAL_HOST_VALUE="${PG_SYNC_TUNNEL_LOCAL_HOST:-${PG_TUNNEL_LOCAL_HOST:-127.0.0.1}}"
PG_SYNC_TUNNEL_LOCAL_PORT_VALUE="${PG_SYNC_TUNNEL_LOCAL_PORT:-15433}"
PG_SYNC_TUNNEL_PID_FILE_VALUE="${PG_SYNC_TUNNEL_PID_FILE:-$LOG_DIR/pg_sync_ssh_tunnel.pid}"
PG_SYNC_TUNNEL_LOG_FILE_VALUE="${PG_SYNC_TUNNEL_LOG_FILE:-$LOG_DIR/pg_sync_ssh_tunnel.log}"

if collector_pg_tunnel_enabled; then
    collector_require_pg_tunnel_env
    if [[ "${PG_SYNC_TUNNEL_KEEPALIVE:-true}" != "true" ]]; then
        trap cleanup_all EXIT
    fi

    PG_TUNNEL_LOCAL_HOST="$PG_SYNC_TUNNEL_LOCAL_HOST_VALUE" \
    PG_TUNNEL_LOCAL_PORT="$PG_SYNC_TUNNEL_LOCAL_PORT_VALUE" \
    PG_TUNNEL_PID_FILE="$PG_SYNC_TUNNEL_PID_FILE_VALUE" \
    PG_TUNNEL_LOG_FILE="$PG_SYNC_TUNNEL_LOG_FILE_VALUE" \
    /bin/bash "$TUNNEL_SCRIPT" start

    export PG_HOST="$PG_SYNC_TUNNEL_LOCAL_HOST_VALUE"
    export PG_PORT="$PG_SYNC_TUNNEL_LOCAL_PORT_VALUE"
fi

cmd=(
    "$PYTHON_BIN"
    "$SYNC_SCRIPT"
    --lock-file "$LOCK_FILE"
)

if [[ -n "$DUCKDB_PATH" ]]; then
    cmd+=(--duckdb-path "$DUCKDB_PATH")
fi

"${cmd[@]}"