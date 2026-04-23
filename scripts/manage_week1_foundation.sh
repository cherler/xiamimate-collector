#!/usr/bin/env bash

set -euo pipefail

export PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/load_collector_env.sh
source "$ROOT_DIR/scripts/load_collector_env.sh"

PYTHON_BIN="${XIAMIMATE_PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
LOG_DIR="${XIAMIMATE_LOG_DIR:-$ROOT_DIR/logs}"
SOURCE_DB="${WEEK1_FOUNDATION_SOURCE_DB:-${XIAMIMATE_DUCKDB_PATH:-${DUCKDB_PATH:-}}}"
OUTPUT_DIR="${WEEK1_FOUNDATION_OUTPUT_DIR:-/Volumes/E/data/xiamimate-data-platform/storage/features/training_sets/week1_foundation}"
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

start_build() {
    if is_running; then
        echo "week1 foundation build already running: PID $(resolve_pid)"
        return 0
    fi

    local command
    command="$(build_command)"
    nohup zsh -lc "cd '$ROOT_DIR' && $command" >> "$LOG_FILE" 2>&1 &
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
    eval "$(build_command)"
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