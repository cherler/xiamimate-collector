#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

TARGET="${1:-all}"

AUTO_UNIT="${XIAMIMATE_AUTO_COLLECT_UNIT:-xiamimate-auto-collect.service}"
PG_UNIT="${XIAMIMATE_PG_SYNC_UNIT:-xiamimate-pg-sync.service}"
THEME_UNIT="${XIAMIMATE_THEME_SYNC_UNIT:-xiamimate-theme-sync.service}"
DUCKDB_PATH="${XIAMIMATE_DUCKDB_PATH:-${DUCKDB_PATH:-/data/xiamimate/duckdb/live/local_analytics.duckdb}}"
WAIT_SECONDS="${XIAMIMATE_DUCKDB_LOCK_WAIT_SECONDS:-30}"

systemd_available() {
    command -v systemctl >/dev/null 2>&1
}

wait_no_duckdb_holders() {
    if ! command -v lsof >/dev/null 2>&1 || [[ ! -e "$DUCKDB_PATH" ]]; then
        return 0
    fi

    local deadline
    deadline=$((SECONDS + WAIT_SECONDS))
    while (( SECONDS < deadline )); do
        if ! lsof "$DUCKDB_PATH" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done

    echo "DuckDB is still held after ${WAIT_SECONDS}s: $DUCKDB_PATH" >&2
    lsof "$DUCKDB_PATH" >&2 || true
    return 1
}

restart_auto() {
    if systemd_available; then
        systemctl start "$AUTO_UNIT" >/dev/null 2>&1 || true
    fi
}

if ! systemd_available; then
    echo "systemd is required for live DuckDB sync window" >&2
    exit 1
fi

case "$TARGET" in
    all|pg|theme)
        ;;
    *)
        echo "usage: bash scripts/run_live_duckdb_sync_window_once.sh [all|pg|theme]" >&2
        exit 1
        ;;
esac

trap restart_auto EXIT

# Stop resident readers first. DuckDB allows multiple readers or one writer across
# processes; keeping pg/theme loops alive prevents auto-collect from taking the
# writer lock on the live database.
systemctl stop "$PG_UNIT" "$THEME_UNIT" >/dev/null 2>&1 || true
systemctl stop "$AUTO_UNIT" >/dev/null 2>&1 || true
wait_no_duckdb_holders

cd "$ROOT_DIR"

if [[ "$TARGET" == "all" || "$TARGET" == "pg" ]]; then
    echo "[$(date '+%F %T %z')] [live-sync-window] pg sync start"
    /bin/bash "$ROOT_DIR/scripts/run_pg_sync_once.sh"
    echo "[$(date '+%F %T %z')] [live-sync-window] pg sync done"
fi

if [[ "$TARGET" == "all" || "$TARGET" == "theme" ]]; then
    echo "[$(date '+%F %T %z')] [live-sync-window] theme sync start"
    /bin/bash "$ROOT_DIR/scripts/run_theme_feature_sync_once.sh"
    echo "[$(date '+%F %T %z')] [live-sync-window] theme sync done"
fi

systemctl start "$AUTO_UNIT"
trap - EXIT

echo "[$(date '+%F %T %z')] [live-sync-window] auto restarted"