#!/usr/bin/env bash

set -euo pipefail

export PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/load_collector_env.sh
source "$ROOT_DIR/scripts/load_collector_env.sh"

PYTHON_BIN="${XIAMIMATE_PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
LOG_DIR="${XIAMIMATE_LOG_DIR:-$ROOT_DIR/logs}"
SOURCE_DB="${WEEK1_FOUNDATION_SOURCE_DB:-${XIAMIMATE_DUCKDB_PATH:-${DUCKDB_PATH:-}}}"
DATA_PLATFORM_ROOT="${XIAMIMATE_DATA_PLATFORM_ROOT:-/Volumes/E/data/xiamimate-data-platform}"
OUTPUT_DIR="${WEEK1_FOUNDATION_OUTPUT_DIR:-$DATA_PLATFORM_ROOT/storage/features/training_sets/week1_foundation}"
SYNC_OUTPUT_DIR="${WEEK1_FOUNDATION_SYNC_OUTPUT_DIR:-/Volumes/pytorch-work/data/week1_foundation}"
TEMP_ROOT="${WEEK1_FOUNDATION_TEMP_ROOT:-$DATA_PLATFORM_ROOT/tmp/week1_foundation}"
TMP_RETENTION_DAYS="${WEEK1_FOUNDATION_TMP_RETENTION_DAYS:-5}"
DOMAINS="${WEEK1_FOUNDATION_DOMAINS:-1,2,3,4,5,6,8,9,10,11,12,13}"
SPLIT_BY_DOMAIN="${WEEK1_FOUNDATION_SPLIT_BY_DOMAIN:-false}"
ACTIVE_ONLY="${WEEK1_FOUNDATION_ACTIVE_ONLY:-true}"
MAX_WORKERS="${WEEK1_FOUNDATION_MAX_WORKERS:-2}"
DUCKDB_THREADS="${WEEK1_FOUNDATION_DUCKDB_THREADS:-1}"
FEATURE_PROFILE="${WEEK1_FOUNDATION_FEATURE_PROFILE:-full}"
EXTRA_ARGS="${WEEK1_FOUNDATION_EXTRA_ARGS:-}"

PID_FILE="$LOG_DIR/week1_foundation.pid"
LOG_FILE="$LOG_DIR/week1_foundation.log"

mkdir -p "$LOG_DIR"

resolve_pid() {
    local pid
    if [[ -f "$PID_FILE" ]]; then
        pid="$(cat "$PID_FILE")"
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            echo "$pid"
            return 0
        fi
    fi

    pid="$(pgrep -f 'data_collector.sales_forecast build-week1-foundation' | head -n 1 || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        echo "$pid" > "$PID_FILE"
        echo "$pid"
        return 0
    fi

    return 1
}

is_running() {
    resolve_pid >/dev/null 2>&1
}

build_command() {
    local cmd=(
        "$PYTHON_BIN"
        -m
        data_collector.sales_forecast
        build-week1-foundation
        --source-db "$SOURCE_DB"
        --output-dir "$OUTPUT_DIR"
        --max-workers "$MAX_WORKERS"
        --duckdb-threads "$DUCKDB_THREADS"
        --feature-profile "$FEATURE_PROFILE"
    )

    if [[ -n "$DOMAINS" ]]; then
        cmd+=(--domains "$DOMAINS")
    elif [[ "$SPLIT_BY_DOMAIN" == "true" ]]; then
        cmd+=(--split-by-domain)
    fi

    if [[ "$ACTIVE_ONLY" == "true" ]]; then
        cmd+=(--active-only)
    fi

    if [[ -n "$EXTRA_ARGS" ]]; then
        # shellcheck disable=SC2206
        local extra=( $EXTRA_ARGS )
        cmd+=("${extra[@]}")
    fi

    printf '%q ' "${cmd[@]}"
}

build_sync_command() {
    local cmd=(
        WEEK1_FOUNDATION_OUTPUT_DIR="$OUTPUT_DIR"
        WEEK1_FOUNDATION_SYNC_OUTPUT_DIR="$SYNC_OUTPUT_DIR"
        WEEK1_FOUNDATION_DOMAINS="$DOMAINS"
        WEEK1_FOUNDATION_SPLIT_BY_DOMAIN="$SPLIT_BY_DOMAIN"
        bash
        "$ROOT_DIR/scripts/manage_week1_foundation.sh"
        sync
    )

    printf '%q ' "${cmd[@]}"
}

cleanup_old_tmp_paths() {
    local stale_path
    local stale_paths=()

    if [[ -z "$TEMP_ROOT" ]]; then
        echo "skip week1 foundation tmp cleanup: WEEK1_FOUNDATION_TEMP_ROOT is empty"
        return 0
    fi

    if [[ ! "$TMP_RETENTION_DAYS" =~ ^[0-9]+$ ]]; then
        echo "invalid WEEK1_FOUNDATION_TMP_RETENTION_DAYS: $TMP_RETENTION_DAYS" >&2
        return 1
    fi

    if [[ ! -d "$TEMP_ROOT" ]]; then
        return 0
    fi

    while IFS= read -r -d '' stale_path; do
        stale_paths+=("$stale_path")
    done < <(find "$TEMP_ROOT" -mindepth 1 -maxdepth 1 -mtime +"$TMP_RETENTION_DAYS" -print0)

    if [[ "${#stale_paths[@]}" -eq 0 ]]; then
        return 0
    fi

    echo "cleanup week1 foundation tmp paths older than ${TMP_RETENTION_DAYS} days under $TEMP_ROOT"
    for stale_path in "${stale_paths[@]}"; do
        echo "remove stale tmp path: $stale_path"
        rm -rf "$stale_path"
    done
}

sync_outputs() {
    local requested_domain
    local domain_dir
    local domain_name
    local sync_entries=()

    if [[ -z "$SYNC_OUTPUT_DIR" ]]; then
        echo "skip week1 foundation sync: WEEK1_FOUNDATION_SYNC_OUTPUT_DIR is empty"
        return 0
    fi

    if [[ "$OUTPUT_DIR" == "$SYNC_OUTPUT_DIR" ]]; then
        echo "skip week1 foundation sync: build output already matches sync target"
        return 0
    fi

    if ! command -v rsync >/dev/null 2>&1; then
        echo "rsync is required to sync week1 foundation outputs" >&2
        return 1
    fi

    mkdir -p "$SYNC_OUTPUT_DIR"

    if [[ -n "$DOMAINS" ]]; then
        IFS=',' read -r -a requested_domains <<< "$DOMAINS"
        for requested_domain in "${requested_domains[@]}"; do
            while IFS= read -r domain_dir; do
                [[ -n "$domain_dir" ]] && sync_entries+=("$domain_dir")
            done < <(find "$OUTPUT_DIR" -maxdepth 1 -type d -name "domain=${requested_domain}_*" | sort)
        done
    else
        while IFS= read -r domain_dir; do
            [[ -n "$domain_dir" ]] && sync_entries+=("$domain_dir")
        done < <(find "$OUTPUT_DIR" -maxdepth 1 -type d -name 'domain=*_*' | sort)
    fi

    if [[ "${#sync_entries[@]}" -eq 0 ]]; then
        echo "sync whole week1 foundation output: $OUTPUT_DIR -> $SYNC_OUTPUT_DIR"
        rsync -a "$OUTPUT_DIR/" "$SYNC_OUTPUT_DIR/"
        return 0
    fi

    for domain_dir in "${sync_entries[@]}"; do
        domain_name="$(basename "$domain_dir")"
        mkdir -p "$SYNC_OUTPUT_DIR/$domain_name"
        echo "sync week1 foundation output: $domain_dir -> $SYNC_OUTPUT_DIR/$domain_name"
        rsync -a "$domain_dir/" "$SYNC_OUTPUT_DIR/$domain_name/"
    done
}

start_build() {
    if is_running; then
        echo "week1 foundation build already running: PID $(resolve_pid)"
        return 0
    fi

    cleanup_old_tmp_paths

    local command
    local sync_command
    command="$(build_command)"
    sync_command="$(build_sync_command)"
    nohup zsh -lc "cd '$ROOT_DIR' && $command && $sync_command" >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"

    sleep 1
    if ! is_running; then
        rm -f "$PID_FILE"
        echo "failed to start week1 foundation build; check log: $LOG_FILE"
        return 1
    fi

    echo "week1 foundation build started: PID $(resolve_pid)"
    echo "log file: $LOG_FILE"
}

run_build() {
    cd "$ROOT_DIR"
    cleanup_old_tmp_paths
    eval "$(build_command)"
    sync_outputs
}

stop_build() {
    if ! is_running; then
        rm -f "$PID_FILE"
        echo "week1 foundation build is not running"
        return 0
    fi

    local pid
    pid="$(resolve_pid)"
    kill "$pid" 2>/dev/null || true
    sleep 1
    if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
    echo "week1 foundation build stopped: PID $pid"
}

status_build() {
    if is_running; then
        echo "week1 foundation build running: PID $(resolve_pid)"
        echo "log file: $LOG_FILE"
    else
        echo "week1 foundation build is not running"
        return 1
    fi
}

show_logs() {
    if [[ ! -f "$LOG_FILE" ]]; then
        echo "log file does not exist yet: $LOG_FILE"
        return 1
    fi
    tail -n 100 "$LOG_FILE"
}

preview_build() {
    echo "python_bin=$PYTHON_BIN"
    echo "source_db=$SOURCE_DB"
    echo "output_dir=$OUTPUT_DIR"
    echo "sync_output_dir=$SYNC_OUTPUT_DIR"
    echo "temp_root=$TEMP_ROOT"
    echo "tmp_retention_days=$TMP_RETENTION_DAYS"
    echo "domains=${DOMAINS:-<auto>}"
    echo "split_by_domain=$SPLIT_BY_DOMAIN"
    echo "active_only=$ACTIVE_ONLY"
    echo "max_workers=$MAX_WORKERS"
    echo "duckdb_threads=$DUCKDB_THREADS"
    echo "feature_profile=$FEATURE_PROFILE"
    echo "log_file=$LOG_FILE"
    echo "command=$(build_command)"
}

usage() {
    cat <<EOF
Usage: bash scripts/manage_week1_foundation.sh {preview|run|start|stop|status|logs|restart}

Environment overrides:
  WEEK1_FOUNDATION_SOURCE_DB
  WEEK1_FOUNDATION_OUTPUT_DIR
    WEEK1_FOUNDATION_SYNC_OUTPUT_DIR
    WEEK1_FOUNDATION_TEMP_ROOT
    WEEK1_FOUNDATION_TMP_RETENTION_DAYS
  WEEK1_FOUNDATION_DOMAINS
  WEEK1_FOUNDATION_SPLIT_BY_DOMAIN
  WEEK1_FOUNDATION_ACTIVE_ONLY
    WEEK1_FOUNDATION_MAX_WORKERS
    WEEK1_FOUNDATION_DUCKDB_THREADS
    WEEK1_FOUNDATION_FEATURE_PROFILE
  WEEK1_FOUNDATION_EXTRA_ARGS
EOF
}

command="${1:-}"
case "$command" in
    preview)
        preview_build
        ;;
    run)
        run_build
        ;;
    sync)
        sync_outputs
        ;;
    start)
        start_build
        ;;
    stop)
        stop_build
        ;;
    status)
        status_build
        ;;
    logs)
        show_logs
        ;;
    restart)
        stop_build || true
        start_build
        ;;
    *)
        usage
        exit 1
        ;;
esac