#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${XIAMIMATE_COLLECTOR_ENV_FILE:-$ROOT_DIR/data_collector/.env}"
SYSTEMD_DIR="${XIAMIMATE_COLLECTOR_SYSTEMD_DIR:-/etc/systemd/system}"
DEFAULT_LOG_DIR="/data/xiamimate/collector/logs"

systemd_available() {
    command -v systemctl >/dev/null 2>&1
}

unit_name_for() {
    case "$1" in
        auto)
            echo "xiamimate-auto-collect.service"
            ;;
        pg-sync)
            echo "xiamimate-pg-sync.service"
            ;;
        theme-sync)
            echo "xiamimate-theme-sync.service"
            ;;
        *)
            echo "unsupported job: $1" >&2
            return 1
            ;;
    esac
}

unit_path_for() {
    local unit_name
    unit_name="$(unit_name_for "$1")"
    echo "$SYSTEMD_DIR/$unit_name"
}

description_for() {
    case "$1" in
        auto)
            echo "XiaMimate Auto Collect"
            ;;
        pg-sync)
            echo "XiaMimate DuckDB to PostgreSQL Sync"
            ;;
        theme-sync)
            echo "XiaMimate Theme Feature Sync"
            ;;
    esac
}

exec_start_for() {
    case "$1" in
        auto)
            echo "/bin/bash $ROOT_DIR/scripts/run_auto_collect_foreground.sh"
            ;;
        pg-sync)
            echo "/bin/bash $ROOT_DIR/scripts/run_pg_sync_loop.sh"
            ;;
        theme-sync)
            echo "/bin/bash $ROOT_DIR/scripts/run_theme_sync_loop.sh"
            ;;
    esac
}

log_file_for() {
    case "$1" in
        auto)
            echo "$DEFAULT_LOG_DIR/auto_collect.service.log"
            ;;
        pg-sync)
            echo "$DEFAULT_LOG_DIR/pg_sync.service.log"
            ;;
        theme-sync)
            echo "$DEFAULT_LOG_DIR/theme_sync.service.log"
            ;;
    esac
}

write_unit() {
    local job="$1"
    local unit_path
    unit_path="$(unit_path_for "$job")"

    cat > "$unit_path" <<EOF
[Unit]
Description=$(description_for "$job")
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$ROOT_DIR
Environment=HOME=/root
Environment=USER=root
Environment=XIAMIMATE_COLLECTOR_ENV_FILE=$ENV_FILE
EnvironmentFile=-$ENV_FILE
ExecStartPre=/bin/mkdir -p $DEFAULT_LOG_DIR
ExecStart=$(exec_start_for "$job")
Restart=always
RestartSec=10
TimeoutStopSec=10min
StandardOutput=append:$(log_file_for "$job")
StandardError=append:$(log_file_for "$job")

[Install]
WantedBy=multi-user.target
EOF
}

install_job() {
    local job="$1"
    local unit_name
    unit_name="$(unit_name_for "$job")"

    write_unit "$job"
    systemctl daemon-reload
    systemctl enable --now "$unit_name"
    echo "installed: $unit_name"
}

uninstall_job() {
    local job="$1"
    local unit_name unit_path
    unit_name="$(unit_name_for "$job")"
    unit_path="$(unit_path_for "$job")"

    systemctl disable --now "$unit_name" >/dev/null 2>&1 || true
    rm -f "$unit_path"
    echo "uninstalled: $unit_name"
}

status_job() {
    local job="$1"
    local unit_name
    unit_name="$(unit_name_for "$job")"

    echo "=== $unit_name ==="
    echo -n "active="
    systemctl is-active "$unit_name" || true
    echo -n "enabled="
    systemctl is-enabled "$unit_name" || true
    echo "journal: journalctl -u $unit_name -n 80 --no-pager"
}

logs_job() {
    local job="$1"
    local unit_name
    unit_name="$(unit_name_for "$job")"
    journalctl -u "$unit_name" -n 80 --no-pager
}

restart_job() {
    local job="$1"
    local unit_name
    unit_name="$(unit_name_for "$job")"
    systemctl restart "$unit_name"
    echo "restarted: $unit_name"
}

run_for_jobs() {
    local action="$1"
    local target="${2:-all}"
    local jobs=()

    if [[ "$target" == "all" ]]; then
        jobs=(auto pg-sync theme-sync)
    else
        jobs=("$target")
    fi

    for job in "${jobs[@]}"; do
        case "$action" in
            install)
                install_job "$job"
                ;;
            uninstall)
                uninstall_job "$job"
                ;;
            status)
                status_job "$job"
                ;;
            logs)
                logs_job "$job"
                ;;
            restart)
                restart_job "$job"
                ;;
            *)
                echo "unsupported action: $action" >&2
                return 1
                ;;
        esac
    done

    if [[ "$action" == "install" || "$action" == "uninstall" ]]; then
        systemctl daemon-reload
        systemctl reset-failed >/dev/null 2>&1 || true
    fi
}

usage() {
    cat <<EOF
Usage:
  bash scripts/manage_ecs2_collector_services.sh install [all|auto|pg-sync|theme-sync]
  bash scripts/manage_ecs2_collector_services.sh uninstall [all|auto|pg-sync|theme-sync]
  bash scripts/manage_ecs2_collector_services.sh status [all|auto|pg-sync|theme-sync]
  bash scripts/manage_ecs2_collector_services.sh restart [all|auto|pg-sync|theme-sync]
  bash scripts/manage_ecs2_collector_services.sh logs <auto|pg-sync|theme-sync>

Environment:
  XIAMIMATE_COLLECTOR_ENV_FILE   default: $ROOT_DIR/data_collector/.env
  XIAMIMATE_COLLECTOR_SYSTEMD_DIR default: /etc/systemd/system
EOF
}

if ! systemd_available; then
    echo "systemd is not available on this host" >&2
    exit 1
fi

case "${1:-}" in
    install|uninstall|status|restart)
        run_for_jobs "$1" "${2:-all}"
        ;;
    logs)
        if [[ -z "${2:-}" ]]; then
            usage >&2
            exit 1
        fi
        logs_job "$2"
        ;;
    *)
        usage >&2
        exit 1
        ;;
esac