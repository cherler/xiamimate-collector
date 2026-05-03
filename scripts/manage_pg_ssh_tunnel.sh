#!/usr/bin/env bash

set -euo pipefail

export PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/load_collector_env.sh
source "$ROOT_DIR/scripts/load_collector_env.sh"

PID_FILE="$(collector_pg_tunnel_pid_file)"
LOG_FILE="$(collector_pg_tunnel_log_file)"
START_LOCK_DIR="${PG_TUNNEL_START_LOCK_DIR:-${XIAMIMATE_LOG_DIR:-$ROOT_DIR/logs}/pg_ssh_tunnel.${PG_TUNNEL_LOCAL_PORT:-15432}.start.lock}"

mkdir -p "${XIAMIMATE_LOG_DIR:-$ROOT_DIR/logs}" "$(dirname "$START_LOCK_DIR")"

cleanup_metadata() {
    rm -f "$PID_FILE"
}

resolve_pid() {
    collector_pg_tunnel_resolve_pid
}

is_running() {
    collector_pg_tunnel_is_running
}

build_command() {
    local cmd=(
        /usr/bin/ssh
        -o ExitOnForwardFailure=yes
        -o ServerAliveInterval=30
        -o ServerAliveCountMax=3
        -N
        -L "${PG_TUNNEL_LOCAL_HOST}:${PG_TUNNEL_LOCAL_PORT}:${PG_TUNNEL_REMOTE_HOST}:${PG_TUNNEL_REMOTE_PORT}"
        "$PG_TUNNEL_SSH_HOST"
    )

    printf '%q ' "${cmd[@]}"
}

acquire_start_lock() {
    local attempt=0

    while ! mkdir "$START_LOCK_DIR" 2>/dev/null; do
        if is_running; then
            return 1
        fi
        if (( attempt >= 50 )); then
            echo "failed to acquire pg tunnel start lock: $START_LOCK_DIR" >&2
            return 2
        fi
        sleep 0.1
        attempt=$((attempt + 1))
    done

    return 0
}

release_start_lock() {
    rmdir "$START_LOCK_DIR" 2>/dev/null || true
}

print_summary() {
    echo "pg_tunnel_enabled=1"
    echo "pg_tunnel=$(collector_pg_tunnel_summary)"
    echo "pg_tunnel_pid_file=$PID_FILE"
    echo "pg_tunnel_log_file=$LOG_FILE"
}

start_tunnel() {
    if ! collector_pg_tunnel_enabled; then
        echo "pg tunnel disabled"
        return 0
    fi

    collector_require_pg_tunnel_env

    if ! command -v ssh >/dev/null 2>&1; then
        echo "ssh not found; cannot create PostgreSQL SSH tunnel" >&2
        return 1
    fi

    if is_running; then
        echo "pg ssh tunnel already running: PID $(resolve_pid)"
        print_summary
        return 0
    fi

    local lock_status
    set +e
    acquire_start_lock
    lock_status=$?
    set -e
    if (( lock_status == 1 )); then
        echo "pg ssh tunnel already running: PID $(resolve_pid)"
        print_summary
        return 0
    fi
    if (( lock_status != 0 )); then
        return 1
    fi

    if is_running; then
        release_start_lock
        echo "pg ssh tunnel already running: PID $(resolve_pid)"
        print_summary
        return 0
    fi

    cleanup_metadata
    local command
    command="$(build_command)"
    nohup zsh -lc "$command" >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"

    sleep 1
    if ! is_running; then
        echo "failed to start pg ssh tunnel; check log: $LOG_FILE" >&2
        cleanup_metadata
        release_start_lock
        return 1
    fi

    echo "pg ssh tunnel started: PID $(resolve_pid)"
    print_summary
    release_start_lock
}

stop_tunnel() {
    if ! collector_pg_tunnel_enabled; then
        echo "pg tunnel disabled"
        return 0
    fi

    if ! is_running; then
        cleanup_metadata
        echo "pg ssh tunnel is not running"
        return 0
    fi

    local pid
    pid="$(resolve_pid)"
    kill "$pid" 2>/dev/null || true

    local attempt=0
    while kill -0 "$pid" 2>/dev/null; do
        if (( attempt >= 50 )); then
            echo "pg ssh tunnel did not stop gracefully; forcing kill: PID $pid"
            kill -9 "$pid" 2>/dev/null || true
            break
        fi
        sleep 0.2
        attempt=$((attempt + 1))
    done

    cleanup_metadata
    echo "pg ssh tunnel stopped: PID $pid"
}

status_tunnel() {
    if ! collector_pg_tunnel_enabled; then
        echo "pg_tunnel_enabled=0"
        return 0
    fi

    if is_running; then
        echo "pg ssh tunnel running: PID $(resolve_pid)"
    else
        echo "pg ssh tunnel is not running"
    fi
    print_summary
}

preview_tunnel() {
    if ! collector_pg_tunnel_enabled; then
        echo "pg_tunnel_enabled=0"
        return 0
    fi

    collector_require_pg_tunnel_env
    print_summary
    echo "command=$(build_command)"
}

ensure_tunnel() {
    if ! collector_pg_tunnel_enabled; then
        return 0
    fi

    if is_running; then
        return 0
    fi

    start_tunnel
}

case "${1:-}" in
    start)
        start_tunnel
        ;;
    stop)
        stop_tunnel
        ;;
    restart)
        stop_tunnel || true
        start_tunnel
        ;;
    status)
        status_tunnel
        ;;
    preview)
        preview_tunnel
        ;;
    ensure)
        ensure_tunnel
        ;;
    *)
        echo "Usage: bash scripts/manage_pg_ssh_tunnel.sh {start|stop|restart|status|preview|ensure}"
        exit 1
        ;;
esac