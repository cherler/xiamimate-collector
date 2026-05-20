#!/usr/bin/env bash

set -euo pipefail

export PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/load_collector_env.sh
source "$ROOT_DIR/scripts/load_collector_env.sh"

LOG_DIR="${XIAMIMATE_LOG_DIR:-/data/xiamimate/collector/logs}"
SNAPSHOT_CURRENT_LINK="${XIAMIMATE_DUCKDB_SNAPSHOT_CURRENT_LINK:-/data/xiamimate/duckdb/snapshots/current}"
SNAPSHOT_DB_PATH="$SNAPSHOT_CURRENT_LINK/local_analytics.duckdb"
DATA_MOUNT="${XIAMIMATE_COLLECTOR_HEALTHCHECK_DATA_MOUNT:-/data}"
DISK_WARN_PCT="${XIAMIMATE_COLLECTOR_HEALTHCHECK_DISK_WARN_PCT:-85}"
DISK_ERROR_PCT="${XIAMIMATE_COLLECTOR_HEALTHCHECK_DISK_ERROR_PCT:-95}"
EXPANSION_SUBSET_DIR="${CANDIDATE_EXPANSION_DUCKDB_SUBSET_DIR:-$LOG_DIR/candidate_expansion_duckdb_subsets}"
EXPANSION_SUBSET_TTL_MINUTES="${CANDIDATE_EXPANSION_DUCKDB_SUBSET_TTL_MINUTES:-360}"

ERRORS=0
WARNINGS=0

error() {
    echo "ERROR: $*"
    ERRORS=$((ERRORS + 1))
}

warn() {
    echo "WARN: $*"
    WARNINGS=$((WARNINGS + 1))
}

ok() {
    echo "OK: $*"
}

unit_active_required() {
    local unit="$1"
    if systemctl is-active --quiet "$unit"; then
        ok "$unit active"
    else
        error "$unit is not active"
        systemctl status "$unit" --no-pager --lines=20 || true
    fi
}

unit_enabled_required() {
    local unit="$1"
    if systemctl is-enabled --quiet "$unit"; then
        ok "$unit enabled"
    else
        error "$unit is not enabled"
    fi
}

unit_not_failed() {
    local unit="$1"
    if systemctl is-failed --quiet "$unit"; then
        error "$unit is failed"
        systemctl status "$unit" --no-pager --lines=30 || true
    else
        ok "$unit not failed"
    fi
}

check_timer() {
    local timer="$1"
    unit_active_required "$timer"
    unit_enabled_required "$timer"
    unit_not_failed "$timer"
    systemctl list-timers "$timer" --no-pager || true
}

check_service() {
    local service="$1"
    local must_be_active="${2:-false}"
    if [[ "$must_be_active" == "true" ]]; then
        unit_active_required "$service"
    else
        unit_not_failed "$service"
    fi
}

check_disk() {
    if [[ ! -d "$DATA_MOUNT" ]]; then
        warn "data mount not found: $DATA_MOUNT"
        return
    fi

    local usage
    usage="$(df -P "$DATA_MOUNT" | awk 'NR==2 {gsub("%", "", $5); print $5}')"
    if [[ -z "$usage" ]]; then
        warn "could not read disk usage for $DATA_MOUNT"
        return
    fi

    if (( usage >= DISK_ERROR_PCT )); then
        error "$DATA_MOUNT disk usage ${usage}% >= ${DISK_ERROR_PCT}%"
    elif (( usage >= DISK_WARN_PCT )); then
        warn "$DATA_MOUNT disk usage ${usage}% >= ${DISK_WARN_PCT}%"
    else
        ok "$DATA_MOUNT disk usage ${usage}%"
    fi
}

check_snapshot() {
    if [[ -f "$SNAPSHOT_DB_PATH" ]]; then
        ok "current DuckDB snapshot exists: $SNAPSHOT_DB_PATH"
        ls -lh "$SNAPSHOT_DB_PATH" || true
    else
        error "current DuckDB snapshot missing: $SNAPSHOT_DB_PATH"
    fi
}

check_log_file() {
    local file="$1"
    if [[ -f "$file" ]]; then
        ok "log exists: $file"
    else
        warn "log missing: $file"
    fi
}

check_timer_log_file() {
    local timer="$1"
    local file="$2"
    local last_trigger

    if [[ -f "$file" ]]; then
        ok "log exists: $file"
        return
    fi

    last_trigger="$(systemctl show "$timer" --property=LastTriggerUSec --value 2>/dev/null || true)"
    if [[ -z "$last_trigger" || "$last_trigger" == "n/a" ]]; then
        ok "log pending first scheduled run: $file"
    else
        warn "log missing after timer trigger: $file"
    fi
}

echo "=== xiamimate collector healthcheck $(date '+%F %T %z') ==="
echo "root_dir=$ROOT_DIR"
echo "log_dir=$LOG_DIR"

if ! command -v systemctl >/dev/null 2>&1; then
    error "systemctl not available"
else
    check_service xiamimate-auto-collect.service true

    check_timer xiamimate-pg-sync-snapshot.timer
    check_service xiamimate-pg-sync-snapshot.service false

    check_timer xiamimate-theme-sync-snapshot.timer
    check_service xiamimate-theme-sync-snapshot.service false

    check_timer xiamimate-pg-agg-sync-snapshot.timer
    check_service xiamimate-pg-agg-sync-snapshot.service false

    check_timer xiamimate-raw-products-cleanup.timer
    check_service xiamimate-raw-products-cleanup.service false

    check_timer xiamimate-forecast-duckdb-snapshot.timer
    check_service xiamimate-forecast-duckdb-snapshot.service false
fi

check_disk
check_snapshot

check_log_file "$LOG_DIR/auto_collect.service.log"
check_log_file "$LOG_DIR/pg_sync.timer.log"
check_timer_log_file xiamimate-theme-sync-snapshot.timer "$LOG_DIR/theme_sync.timer.log"
check_timer_log_file xiamimate-pg-agg-sync-snapshot.timer "$LOG_DIR/pg_agg_sync.timer.log"

prune_candidate_expansion_subsets() {
    if [[ ! -d "$EXPANSION_SUBSET_DIR" ]]; then
        return
    fi
    if [[ ! "$EXPANSION_SUBSET_TTL_MINUTES" =~ ^[0-9]+$ ]] || (( EXPANSION_SUBSET_TTL_MINUTES <= 0 )); then
        return
    fi
    # Subset DuckDBs are normally cleaned by run_candidate_expansion_refresh_once.sh's EXIT trap.
    # Anything still present beyond the TTL is leftover from a crashed build and safe to remove.
    local pruned
    pruned="$(find "$EXPANSION_SUBSET_DIR" -maxdepth 1 -type f \
        \( -name '*.duckdb' -o -name '*.duckdb.wal' -o -name '*.manifest.json' \) \
        -mmin +"$EXPANSION_SUBSET_TTL_MINUTES" -print -delete 2>/dev/null | wc -l | tr -d ' ')"
    if [[ -n "$pruned" && "$pruned" != "0" ]]; then
        warn "pruned $pruned stale candidate-expansion subset files older than ${EXPANSION_SUBSET_TTL_MINUTES}min from $EXPANSION_SUBSET_DIR"
    else
        ok "candidate expansion subset dir clean: $EXPANSION_SUBSET_DIR (ttl=${EXPANSION_SUBSET_TTL_MINUTES}min)"
    fi
}

prune_candidate_expansion_subsets

echo "=== summary: errors=$ERRORS warnings=$WARNINGS ==="

if (( ERRORS > 0 )); then
    exit 2
fi

exit 0
