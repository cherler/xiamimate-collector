#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/load_collector_env.sh
source "$ROOT_DIR/scripts/load_collector_env.sh"

TUNNEL_SCRIPT="$ROOT_DIR/scripts/manage_pg_ssh_tunnel.sh"
PYTHON_BIN="${XIAMIMATE_PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
SYNC_SCRIPT="$ROOT_DIR/data_collector/sync_theme_features_to_pg.py"
LOG_DIR="${XIAMIMATE_LOG_DIR:-$ROOT_DIR/logs}"
LOCK_FILE="$LOG_DIR/sync_theme_features_to_pg.lock"
DUCKDB_PATH="${THEME_FEATURE_SYNC_DUCKDB_PATH:-${XIAMIMATE_DUCKDB_PATH:-${DUCKDB_PATH:-}}}"

mkdir -p "$LOG_DIR"

cleanup_tunnel() {
    if collector_pg_tunnel_enabled; then
        PG_TUNNEL_LOCAL_HOST="$THEME_SYNC_TUNNEL_LOCAL_HOST_VALUE" \
        PG_TUNNEL_LOCAL_PORT="$THEME_SYNC_TUNNEL_LOCAL_PORT_VALUE" \
        PG_TUNNEL_PID_FILE="$THEME_SYNC_TUNNEL_PID_FILE_VALUE" \
        PG_TUNNEL_LOG_FILE="$THEME_SYNC_TUNNEL_LOG_FILE_VALUE" \
        bash "$TUNNEL_SCRIPT" stop >/dev/null 2>&1 || true
    fi
}

collector_require_pg_env

THEME_SYNC_TUNNEL_LOCAL_HOST_VALUE="${THEME_FEATURE_SYNC_TUNNEL_LOCAL_HOST:-${PG_TUNNEL_LOCAL_HOST:-127.0.0.1}}"
THEME_SYNC_TUNNEL_LOCAL_PORT_VALUE="${THEME_FEATURE_SYNC_TUNNEL_LOCAL_PORT:-15433}"
THEME_SYNC_TUNNEL_PID_FILE_VALUE="${THEME_FEATURE_SYNC_TUNNEL_PID_FILE:-$LOG_DIR/theme_feature_sync_ssh_tunnel.pid}"
THEME_SYNC_TUNNEL_LOG_FILE_VALUE="${THEME_FEATURE_SYNC_TUNNEL_LOG_FILE:-$LOG_DIR/theme_feature_sync_ssh_tunnel.log}"

if collector_pg_tunnel_enabled; then
    collector_require_pg_tunnel_env
    trap cleanup_tunnel EXIT

    PG_TUNNEL_LOCAL_HOST="$THEME_SYNC_TUNNEL_LOCAL_HOST_VALUE" \
    PG_TUNNEL_LOCAL_PORT="$THEME_SYNC_TUNNEL_LOCAL_PORT_VALUE" \
    PG_TUNNEL_PID_FILE="$THEME_SYNC_TUNNEL_PID_FILE_VALUE" \
    PG_TUNNEL_LOG_FILE="$THEME_SYNC_TUNNEL_LOG_FILE_VALUE" \
    bash "$TUNNEL_SCRIPT" start

    export PG_HOST="$THEME_SYNC_TUNNEL_LOCAL_HOST_VALUE"
    export PG_PORT="$THEME_SYNC_TUNNEL_LOCAL_PORT_VALUE"
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