#!/usr/bin/env bash

set -euo pipefail

export PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

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

PLIST_NAME="com.xiamimate.pg-sync"
PLIST_DST="$HOME/Library/LaunchAgents/${PLIST_NAME}.plist"

mkdir -p "$LOG_DIR"

SYNC_TUNNEL_LOCAL_HOST="${PG_SYNC_TUNNEL_LOCAL_HOST:-${PG_TUNNEL_LOCAL_HOST:-127.0.0.1}}"
SYNC_TUNNEL_LOCAL_PORT="${PG_SYNC_TUNNEL_LOCAL_PORT:-15433}"
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
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            lsof -nP -a -p "$pid" -iTCP:"$SYNC_TUNNEL_LOCAL_PORT" -sTCP:LISTEN >/dev/null 2>&1
            return $?
        fi
    fi
    rm -f "$SYNC_TUNNEL_PID_FILE"
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
        /bin/bash
        "$RUN_ONCE_SCRIPT"
    )

    printf '%q ' "${cmd[@]}"
}

build_loop_command() {
    local once_command
    once_command="$(build_command)"
    printf 'while true; do %s; sleep %q; done' "$once_command" "$INTERVAL"
}

is_launchd_loaded() {
    launchctl list "$PLIST_NAME" >/dev/null 2>&1
}

launchd_pid() {
    launchctl list "$PLIST_NAME" 2>/dev/null | sed -n 's/.*"PID" = \([0-9][0-9]*\);/\1/p' | head -n 1
}

write_launchd_plist() {
    local loop_command
    loop_command="$(build_loop_command)"

    cat > "$PLIST_DST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${PLIST_NAME}</string>

    <key>WorkingDirectory</key>
    <string>${ROOT_DIR}</string>

    <key>ProgramArguments</key>
    <array>
        <string>/bin/zsh</string>
        <string>-lc</string>
        <string>cd '${ROOT_DIR}' &amp;&amp; ${loop_command}</string>
    </array>

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
        <key>PYTHONPATH</key>
        <string>${ROOT_DIR}</string>
    </dict>

    <key>KeepAlive</key>
    <true/>

    <key>ThrottleInterval</key>
    <integer>10</integer>

    <key>StandardOutPath</key>
    <string>${LOG_FILE}</string>
    <key>StandardErrorPath</key>
    <string>${LOG_FILE}</string>

    <key>AbandonProcessGroup</key>
    <false/>
</dict>
</plist>
EOF
}

install_launchd() {
    collector_require_pg_env
    collector_require_pg_tunnel_env

    if is_running && ! is_launchd_loaded; then
        echo "stopping existing nohup process first..."
        stop_sync || true
    fi

    if is_launchd_loaded; then
        launchctl unload "$PLIST_DST" 2>/dev/null || true
    fi

    write_launchd_plist
    launchctl load "$PLIST_DST"

    sleep 2
    if is_launchd_loaded; then
        local pid
        pid="$(launchd_pid)"
        echo "launchd 已安装并启动: $PLIST_NAME (PID ${pid:-pending})"
        echo "  开机自启: YES"
        echo "  崩溃重启: YES"
        echo "  休眠恢复: YES"
        echo "  日志文件: $LOG_FILE"
        echo ""
        echo "管理命令:"
        echo "  状态: bash scripts/manage_pg_sync.sh status"
        echo "  停止: bash scripts/manage_pg_sync.sh stop"
        echo "  启动: bash scripts/manage_pg_sync.sh start"
        echo "  卸载: bash scripts/manage_pg_sync.sh uninstall"
    else
        echo "launchd 安装失败; check: launchctl list | grep ${PLIST_NAME}"
        return 1
    fi
}

uninstall_launchd() {
    if is_launchd_loaded; then
        launchctl unload "$PLIST_DST" 2>/dev/null || true
        echo "launchd 已卸载: $PLIST_NAME"
    else
        echo "launchd 未安装"
    fi
    rm -f "$PLIST_DST"
    cleanup_metadata
}

start_via_launchd() {
    if [[ -f "$PLIST_DST" ]]; then
        if ! is_launchd_loaded; then
            launchctl load "$PLIST_DST"
        fi
        launchctl start "$PLIST_NAME"
        sleep 1
        echo "pg sync started via launchd"
        return 0
    fi
    return 1
}

stop_via_launchd() {
    if is_launchd_loaded; then
        launchctl stop "$PLIST_NAME"
        echo "pg sync stopped via launchd (will auto-restart due to KeepAlive)"
        echo "  若要彻底停止: bash scripts/manage_pg_sync.sh uninstall"
        return 0
    fi
    return 1
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
    nohup /bin/zsh -lc "cd '$ROOT_DIR' && $command" >> "$LOG_FILE" 2>&1 &
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
    echo "plist_name=$PLIST_NAME"
    echo "plist_dst=$PLIST_DST"
    print_pg_target
    print_pg_tunnel
    echo "sync_once_command=$(build_command)"
    echo "loop_command=$(build_loop_command)"
}

case "${1:-}" in
    start)
        if start_via_launchd 2>/dev/null; then
            :
        else
            start_sync
        fi
        ;;
    stop)
        if is_launchd_loaded; then
            stop_via_launchd
        else
            stop_sync
        fi
        ;;
    restart)
        if is_launchd_loaded; then
            launchctl stop "$PLIST_NAME"
            sleep 2
            echo "pg sync restarted via launchd"
        else
            restart_sync
        fi
        ;;
    status)
        if is_launchd_loaded; then
            echo "pg sync managed by launchd: $PLIST_NAME"
            launchctl list "$PLIST_NAME" 2>/dev/null
            echo "log file: $LOG_FILE"
            if collector_require_pg_env >/dev/null 2>&1; then
                print_pg_target
                print_pg_tunnel
            fi
        else
            status_sync
        fi
        ;;
    install)
        install_launchd
        ;;
    uninstall)
        uninstall_launchd
        ;;
    logs)
        show_logs
        ;;
    preview)
        preview_sync
        ;;
    *)
        echo "Usage: bash scripts/manage_pg_sync.sh {start|stop|restart|status|install|uninstall|logs|preview}"
        echo ""
        echo "  install    安装 launchd 服务 (推荐: 开机自启+崩溃重启+休眠恢复)"
        echo "  uninstall  卸载 launchd 服务"
        echo "  start      启动 (优先 launchd, 否则 nohup)"
        echo "  stop       停止"
        echo "  restart    重启"
        echo "  status     查看状态"
        echo "  logs       查看最近日志"
        echo "  preview    仅打印解析后的命令与路径, 不启动进程"
        exit 1
        ;;
esac