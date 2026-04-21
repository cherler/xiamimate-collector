#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/load_collector_env.sh
source "$ROOT_DIR/scripts/load_collector_env.sh"
TUNNEL_SCRIPT="$ROOT_DIR/scripts/manage_pg_ssh_tunnel.sh"
RUN_ONCE_SCRIPT="$ROOT_DIR/scripts/run_pg_sync_once.sh"
PYTHON_BIN="${XIAMIMATE_PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
SYNC_SCRIPT="$ROOT_DIR/data_collector/sync_duckdb_to_pg.py"
LOG_DIR="${XIAMIMATE_LOG_DIR:-$ROOT_DIR/logs}"
PID_FILE="$LOG_DIR/sync_duckdb_to_pg.pid"
LOG_FILE="$LOG_DIR/sync_duckdb_to_pg.log"
LOCK_FILE="$LOG_DIR/sync_duckdb_to_pg.lock"
INTERVAL="${SYNC_INTERVAL_SECONDS:-300}"
DUCKDB_PATH="${PG_SYNC_DUCKDB_PATH:-${XIAMIMATE_DUCKDB_PATH:-${DUCKDB_PATH:-}}}"

mkdir -p "$LOG_DIR"

SYNC_TUNNEL_LOCAL_HOST="${PG_SYNC_TUNNEL_LOCAL_HOST:-${PG_TUNNEL_LOCAL_HOST:-127.0.0.1}}"
SYNC_TUNNEL_LOCAL_PORT="${PG_SYNC_TUNNEL_LOCAL_PORT:-15432}"
SYNC_TUNNEL_PID_FILE="${PG_SYNC_TUNNEL_PID_FILE:-$LOG_DIR/pg_sync_ssh_tunnel.pid}"
SYNC_TUNNEL_LOG_FILE="${PG_SYNC_TUNNEL_LOG_FILE:-$LOG_DIR/pg_sync_ssh_tunnel.log}"

cleanup_metadata() {
    rm -f "$PID_FILE" "$LOCK_FILE"
}

wait_for_shutdown() {
    local pid="$1"
    local attempts="${2:-50}"
    local interval_seconds="${3:-0.2}"
    local attempt=0

    while kill -0 "$pid" 2>/dev/null; do
        if (( attempt >= attempts )); then
            return 1
        fi
        sleep "$interval_seconds"
        attempt=$((attempt + 1))
    done

    return 0
}

resolve_pid() {
    local pid
    if [[ -f "$PID_FILE" ]]; then
        pid="$(cat "$PID_FILE")"
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            echo "$pid"
            return 0
        fi
    fi

    if [[ -f "$LOCK_FILE" ]]; then
        pid="$(sed -n 's/^pid=//p' "$LOCK_FILE" | head -n 1)"
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            echo "$pid" > "$PID_FILE"
            echo "$pid"
            return 0
        fi
    fi

    return 1
}

is_running() {
    resolve_pid >/dev/null 2>&1
}

tunnel_is_running() {
    local pid
    if [[ -f "$SYNC_TUNNEL_PID_FILE" ]]; then
        pid="$(cat "$SYNC_TUNNEL_PID_FILE")"
        [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
        return $?
    fi
    return 1
}

print_pg_target() {
    local host="${PG_HOST:-<unset>}"
    local port="${PG_PORT:-5432}"
    local db="${PG_DB:-<unset>}"
    local user="${PG_USER:-<unset>}"

    if collector_pg_tunnel_enabled; then
        host="$SYNC_TUNNEL_LOCAL_HOST"
        port="$SYNC_TUNNEL_LOCAL_PORT"
        echo "pg_target=${host}:${port}/${db} as ${user} (ssh tunnel -> ${PG_TUNNEL_REMOTE_HOST:-<unset>}:${PG_TUNNEL_REMOTE_PORT:-<unset>} via ${PG_TUNNEL_SSH_HOST:-<unset>})"
        echo "pg_target_mode=local"
    else
        echo "pg_target=${host}:${port}/${db} as ${user}"
        echo "pg_target_mode=remote"
    fi
}

print_pg_tunnel() {
    if collector_pg_tunnel_enabled; then
        echo "pg_tunnel=${SYNC_TUNNEL_LOCAL_HOST}:${SYNC_TUNNEL_LOCAL_PORT} via ${PG_TUNNEL_SSH_HOST:-<unset>} -> ${PG_TUNNEL_REMOTE_HOST:-<unset>}:${PG_TUNNEL_REMOTE_PORT:-<unset>}"
        echo "pg_tunnel_pid_file=$SYNC_TUNNEL_PID_FILE"
        echo "pg_tunnel_log_file=$SYNC_TUNNEL_LOG_FILE"
        if tunnel_is_running; then
            echo "pg_tunnel_status=running"
        else
            echo "pg_tunnel_status=stopped"
        fi
    else
        echo "pg_tunnel=disabled"
    fi
}

build_command() {
    local cmd=(
        bash
        "$RUN_ONCE_SCRIPT"
    )

    printf '%q ' "${cmd[@]}"
}

build_loop_command() {
    local once_command
    once_command="$(build_command)"
    printf 'while true; do %s; sleep %q; done' "$once_command" "$INTERVAL"
}

start_sync() {
    collector_require_pg_env
    collector_require_pg_tunnel_env

    if is_running; then
        echo "sync loop already running: PID $(resolve_pid)"
        print_pg_target
        print_pg_tunnel
        return 0
    fi

    cleanup_metadata
    local command
    command="$(build_loop_command)"
    nohup zsh -lc "cd '$ROOT_DIR' && $command" >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"

    sleep 1
    if ! is_running; then
        echo "failed to start sync loop; check log: $LOG_FILE"
        cleanup_metadata
        return 1
    fi

    echo "sync loop started: PID $(cat "$PID_FILE")"
    echo "log file: $LOG_FILE"
    print_pg_target
    print_pg_tunnel
}

stop_sync() {
    if ! is_running; then
        cleanup_metadata
        echo "sync loop is not running"
        return 0
    fi

    local pid
    pid="$(resolve_pid)"

    kill -TERM -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
    if ! wait_for_shutdown "$pid"; then
        echo "sync loop did not stop gracefully; forcing kill: PID $pid"
        kill -9 -- "-$pid" 2>/dev/null || kill -9 "$pid" 2>/dev/null || true
        if ! wait_for_shutdown "$pid" 25 0.2; then
            echo "failed to stop sync loop: PID $pid"
            return 1
        fi
    fi

    cleanup_metadata
    echo "sync loop stopped: PID $pid"
}

status_sync() {
    if is_running; then
        echo "sync loop running: PID $(resolve_pid)"
        echo "log file: $LOG_FILE"
        if collector_require_pg_env >/dev/null 2>&1; then
            print_pg_target
            print_pg_tunnel
        fi
    else
        echo "sync loop is not running"
        return 1
    fi
}

restart_sync() {
    stop_sync || true
    start_sync
}

show_logs() {
    if [[ ! -f "$LOG_FILE" ]]; then
        echo "log file does not exist yet: $LOG_FILE"
        return 1
    fi
    tail -n 50 "$LOG_FILE"
}

preview_sync() {
    collector_require_pg_env

    echo "python_bin=$PYTHON_BIN"
    echo "log_dir=$LOG_DIR"
    echo "lock_file=$LOCK_FILE"
    echo "duckdb_path=${DUCKDB_PATH:-<default>}"
    echo "interval_seconds=$INTERVAL"
    print_pg_target
    print_pg_tunnel
    echo "sync_once_command=$(build_command)"
    echo "loop_command=$(build_loop_command)"
}

case "${1:-}" in
    start)
        start_sync
        ;;
    stop)
        stop_sync
        ;;
    restart)
        restart_sync
        ;;
    status)
        status_sync
        ;;
    logs)
        show_logs
        ;;
    preview)
        preview_sync
        ;;
    *)
        echo "Usage: bash scripts/manage_pg_sync.sh {start|stop|restart|status|logs|preview}"
        exit 1
        ;;
esac