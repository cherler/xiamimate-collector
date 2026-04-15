#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/load_collector_env.sh
source "$ROOT_DIR/scripts/load_collector_env.sh"
PYTHON_BIN="${XIAMIMATE_PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
LOG_DIR="${XIAMIMATE_LOG_DIR:-$ROOT_DIR/logs}"
PID_FILE="$LOG_DIR/auto_collect.pid"
LOCK_FILE="$LOG_DIR/auto_collect.lock"
LOG_FILE="$LOG_DIR/auto_collect.log"

PLIST_NAME="com.xiamimate.auto-collect"
PLIST_SRC="$ROOT_DIR/scripts/${PLIST_NAME}.plist"
PLIST_DST="$HOME/Library/LaunchAgents/${PLIST_NAME}.plist"

DOMAIN="${AUTO_COLLECT_DOMAIN:-all}"
INTERVAL_MINUTES="${AUTO_COLLECT_INTERVAL_MINUTES:-3}"
BATCH_SIZE="${AUTO_COLLECT_BATCH_SIZE:-50}"
STALE_HOURS="${AUTO_COLLECT_STALE_HOURS:-336}"
ENABLE_TRENDS="${AUTO_COLLECT_ENABLE_TRENDS:-true}"
ENABLE_STRATEGY_EXPANSION="${AUTO_COLLECT_ENABLE_STRATEGY_EXPANSION:-true}"
STRATEGY_PENDING_THRESHOLD="${AUTO_COLLECT_STRATEGY_PENDING_THRESHOLD:-200}"
STRATEGY_CATEGORY_LIMIT="${AUTO_COLLECT_STRATEGY_CATEGORY_LIMIT:-2}"
STRATEGY_KEYWORD_LIMIT="${AUTO_COLLECT_STRATEGY_KEYWORD_LIMIT:-5}"
STRATEGY_CATEGORY_COOLDOWN_HOURS="${AUTO_COLLECT_STRATEGY_CATEGORY_COOLDOWN_HOURS:-720}"
STRATEGY_KEYWORD_COOLDOWN_HOURS="${AUTO_COLLECT_STRATEGY_KEYWORD_COOLDOWN_HOURS:-72}"
DB_PATH="${AUTO_COLLECT_DB_PATH:-${XIAMIMATE_DUCKDB_PATH:-${DUCKDB_PATH:-}}}"
EXTRA_ARGS="${AUTO_COLLECT_EXTRA_ARGS:-}"

mkdir -p "$LOG_DIR"

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
    if [[ -f "$LOCK_FILE" ]]; then
        pid="$(sed -n 's/^pid=//p' "$LOCK_FILE" | head -n 1)"
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            echo "$pid" > "$PID_FILE"
            echo "$pid"
            return 0
        fi
    fi

    if [[ -f "$PID_FILE" ]]; then
        pid="$(cat "$PID_FILE")"
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            echo "$pid"
            return 0
        fi
    fi

    pid="$(pgrep -f "data_collector.cross_border_data auto-collect" | head -n 1 || true)"
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

get_process_command() {
    local pid="$1"
    ps -ww -p "$pid" -o command= | head -n 1
}

extract_flag_value() {
    local command_line="$1"
    local flag="$2"
    local fallback="$3"
    local value

    value="$(printf '%s\n' "$command_line" | sed -n "s/.*${flag} \([^ ]*\).*/\1/p" | head -n 1)"
    if [[ -n "$value" ]]; then
        echo "$value"
    else
        echo "$fallback"
    fi
}

extract_bool_flag() {
    local command_line="$1"
    local positive_flag="$2"
    local negative_flag="$3"
    local fallback="$4"

    if [[ "$command_line" == *"$positive_flag"* ]]; then
        echo "true"
    elif [[ -n "$negative_flag" && "$command_line" == *"$negative_flag"* ]]; then
        echo "false"
    else
        echo "$fallback"
    fi
}

build_command() {
    local cmd=(
        "$PYTHON_BIN"
        -m
        data_collector.cross_border_data
        auto-collect
        --domain "$DOMAIN"
        --loop
        --interval-minutes "$INTERVAL_MINUTES"
        --batch-size "$BATCH_SIZE"
        --stale-hours "$STALE_HOURS"
        --lock-file "$LOCK_FILE"
    )

    if [[ "$ENABLE_TRENDS" == "true" ]]; then
        cmd+=(--enable-trends)
    else
        cmd+=(--disable-trends)
    fi

    if [[ "$ENABLE_STRATEGY_EXPANSION" == "true" ]]; then
        cmd+=(
            --enable-strategy-expansion
            --strategy-pending-threshold "$STRATEGY_PENDING_THRESHOLD"
            --strategy-category-limit "$STRATEGY_CATEGORY_LIMIT"
            --strategy-keyword-limit "$STRATEGY_KEYWORD_LIMIT"
            --strategy-category-cooldown-hours "$STRATEGY_CATEGORY_COOLDOWN_HOURS"
            --strategy-keyword-cooldown-hours "$STRATEGY_KEYWORD_COOLDOWN_HOURS"
        )
    fi

    if [[ -n "$DB_PATH" ]]; then
        cmd+=(--db-path "$DB_PATH")
    fi

    if [[ -n "$EXTRA_ARGS" ]]; then
        # shellcheck disable=SC2206
        local extra=( $EXTRA_ARGS )
        cmd+=("${extra[@]}")
    fi

    printf '%q ' "${cmd[@]}"
}

start_auto_collect() {
    if is_running; then
        echo "auto-collect already running: PID $(resolve_pid)"
        return 0
    fi

    cleanup_metadata

    local command
    command="$(build_command)"
    nohup env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY \
        zsh -lc "cd '$ROOT_DIR' && $command" >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"

    sleep 1
    if ! is_running; then
        echo "failed to start auto-collect; check log: $LOG_FILE"
        cleanup_metadata
        return 1
    fi

    echo "auto-collect started: PID $(resolve_pid)"
    echo "log file: $LOG_FILE"
}

stop_auto_collect() {
    if ! is_running; then
        cleanup_metadata
        echo "auto-collect is not running"
        return 0
    fi

    local pid
    pid="$(resolve_pid)"

    kill "$pid" 2>/dev/null || true
    if ! wait_for_shutdown "$pid"; then
        echo "auto-collect did not stop gracefully; forcing kill: PID $pid"
        kill -9 "$pid" 2>/dev/null || true
        if ! wait_for_shutdown "$pid" 25 0.2; then
            echo "failed to stop auto-collect: PID $pid"
            return 1
        fi
    fi

    cleanup_metadata
    echo "auto-collect stopped: PID $pid"
}

status_auto_collect() {
    if is_running; then
        local pid command_line domain interval_minutes batch_size stale_hours trends strategy_expansion strategy_pending_threshold strategy_category_limit strategy_keyword_limit strategy_category_cooldown_hours strategy_keyword_cooldown_hours
        pid="$(resolve_pid)"
        command_line="$(get_process_command "$pid")"
        domain="$(extract_flag_value "$command_line" '--domain' "$DOMAIN")"
        interval_minutes="$(extract_flag_value "$command_line" '--interval-minutes' "$INTERVAL_MINUTES")"
        batch_size="$(extract_flag_value "$command_line" '--batch-size' "$BATCH_SIZE")"
        stale_hours="$(extract_flag_value "$command_line" '--stale-hours' "$STALE_HOURS")"
        trends="$(extract_bool_flag "$command_line" '--enable-trends' '--disable-trends' "$ENABLE_TRENDS")"
        strategy_expansion="$(extract_bool_flag "$command_line" '--enable-strategy-expansion' '' "$ENABLE_STRATEGY_EXPANSION")"
        strategy_pending_threshold="$(extract_flag_value "$command_line" '--strategy-pending-threshold' "$STRATEGY_PENDING_THRESHOLD")"
        strategy_category_limit="$(extract_flag_value "$command_line" '--strategy-category-limit' "$STRATEGY_CATEGORY_LIMIT")"
        strategy_keyword_limit="$(extract_flag_value "$command_line" '--strategy-keyword-limit' "$STRATEGY_KEYWORD_LIMIT")"
        strategy_category_cooldown_hours="$(extract_flag_value "$command_line" '--strategy-category-cooldown-hours' "$STRATEGY_CATEGORY_COOLDOWN_HOURS")"
        strategy_keyword_cooldown_hours="$(extract_flag_value "$command_line" '--strategy-keyword-cooldown-hours' "$STRATEGY_KEYWORD_COOLDOWN_HOURS")"

        echo "auto-collect running: PID $pid"
        echo "domain=$domain interval_minutes=$interval_minutes batch_size=$batch_size stale_hours=$stale_hours trends=$trends strategy_expansion=$strategy_expansion strategy_pending_threshold=$strategy_pending_threshold strategy_category_limit=$strategy_category_limit strategy_keyword_limit=$strategy_keyword_limit strategy_category_cooldown_hours=$strategy_category_cooldown_hours strategy_keyword_cooldown_hours=$strategy_keyword_cooldown_hours"
        echo "log file: $LOG_FILE"
    else
        echo "auto-collect is not running"
        return 1
    fi
}

restart_auto_collect() {
    stop_auto_collect || true
    start_auto_collect
}

show_logs() {
    if [[ ! -f "$LOG_FILE" ]]; then
        echo "log file does not exist yet: $LOG_FILE"
        return 1
    fi
    tail -n 50 "$LOG_FILE"
}

preview_auto_collect() {
    echo "python_bin=$PYTHON_BIN"
    echo "log_dir=$LOG_DIR"
    echo "lock_file=$LOCK_FILE"
    echo "duckdb_path=${DB_PATH:-<default>}"
    echo "command=$(build_command)"
}

# ------------------------------------------------------------------
# launchd 管理 (推荐: 休眠恢复 + 崩溃自动重启)
# ------------------------------------------------------------------

is_launchd_loaded() {
    launchctl list "$PLIST_NAME" >/dev/null 2>&1
}

install_launchd() {
    if [[ ! -f "$PLIST_SRC" ]]; then
        echo "plist not found: $PLIST_SRC"
        return 1
    fi

    # 先停掉旧的 nohup 进程 (如果有)
    if is_running; then
        echo "stopping existing nohup process first..."
        stop_auto_collect || true
    fi

    # 卸载旧 plist (如果已加载)
    if is_launchd_loaded; then
        launchctl unload "$PLIST_DST" 2>/dev/null || true
    fi

    cp "$PLIST_SRC" "$PLIST_DST"
    launchctl load "$PLIST_DST"

    sleep 2
    if is_launchd_loaded; then
        local pid
        pid="$(launchctl list "$PLIST_NAME" 2>/dev/null | awk 'NR==2{print $1}')"
        echo "launchd 已安装并启动: $PLIST_NAME (PID ${pid:-pending})"
        echo "  开机自启: YES"
        echo "  崩溃重启: YES"
        echo "  休眠恢复: YES"
        echo "  日志文件: $LOG_FILE"
        echo ""
        echo "管理命令:"
        echo "  状态: bash scripts/manage_auto_collect.sh status"
        echo "  停止: bash scripts/manage_auto_collect.sh stop"
        echo "  启动: bash scripts/manage_auto_collect.sh start"
        echo "  卸载: bash scripts/manage_auto_collect.sh uninstall"
    else
        echo "launchd 安装失败; check: launchctl list | grep xiamimate"
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
        echo "auto-collect started via launchd"
        return 0
    fi
    return 1
}

stop_via_launchd() {
    if is_launchd_loaded; then
        launchctl stop "$PLIST_NAME"
        echo "auto-collect stopped via launchd (will auto-restart due to KeepAlive)"
        echo "  若要彻底停止: bash scripts/manage_auto_collect.sh uninstall"
        return 0
    fi
    return 1
}

case "${1:-}" in
    start)
        if start_via_launchd 2>/dev/null; then
            :
        else
            start_auto_collect
        fi
        ;;
    stop)
        if is_launchd_loaded; then
            stop_via_launchd
        else
            stop_auto_collect
        fi
        ;;
    restart)
        if is_launchd_loaded; then
            launchctl stop "$PLIST_NAME"
            sleep 2
            echo "auto-collect restarted via launchd"
        else
            restart_auto_collect
        fi
        ;;
    status)
        if is_launchd_loaded; then
            echo "auto-collect managed by launchd: $PLIST_NAME"
            launchctl list "$PLIST_NAME" 2>/dev/null
            echo "log file: $LOG_FILE"
        else
            status_auto_collect
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
        preview_auto_collect
        ;;
    *)
        echo "Usage: bash scripts/manage_auto_collect.sh {start|stop|restart|status|install|uninstall|logs|preview}"
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
