#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${XIAMIMATE_COLLECTOR_ENV_FILE:-$ROOT_DIR/data_collector/.env}"
SYSTEMD_DIR="${XIAMIMATE_COLLECTOR_SYSTEMD_DIR:-/etc/systemd/system}"

systemd_available() {
    command -v systemctl >/dev/null 2>&1
}

service_name_for() {
    case "$1" in
        raw-cleanup)
            echo "xiamimate-raw-products-cleanup.service"
            ;;
        duckdb-snapshot)
            echo "xiamimate-duckdb-snapshot.service"
            ;;
        *)
            echo "unsupported job: $1" >&2
            return 1
            ;;
    esac
}

timer_name_for() {
    case "$1" in
        raw-cleanup)
            echo "xiamimate-raw-products-cleanup.timer"
            ;;
        duckdb-snapshot)
            echo "xiamimate-duckdb-snapshot.timer"
            ;;
        *)
            echo "unsupported job: $1" >&2
            return 1
            ;;
    esac
}

service_path_for() {
    echo "$SYSTEMD_DIR/$(service_name_for "$1")"
}

timer_path_for() {
    echo "$SYSTEMD_DIR/$(timer_name_for "$1")"
}

description_for() {
    case "$1" in
        raw-cleanup)
            echo "XiaMimate Raw Products Cleanup"
            ;;
        duckdb-snapshot)
            echo "XiaMimate DuckDB Snapshot Publisher"
            ;;
    esac
}

exec_start_for() {
    case "$1" in
        raw-cleanup)
            echo "/bin/bash $ROOT_DIR/scripts/cleanup_raw_products.sh --apply"
            ;;
        duckdb-snapshot)
            echo "/bin/bash $ROOT_DIR/scripts/publish_duckdb_snapshot.sh"
            ;;
    esac
}

schedule_for() {
    case "$1" in
        raw-cleanup)
            echo "*-*-* 03:20:00"
            ;;
        duckdb-snapshot)
            echo "Sun *-*-* 02:00:00"
            ;;
    esac
}

write_service() {
    local job="$1"
    local service_path
    service_path="$(service_path_for "$job")"

    cat > "$service_path" <<EOF
[Unit]
Description=$(description_for "$job")
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
WorkingDirectory=$ROOT_DIR
Environment=HOME=/root
Environment=USER=root
Environment=XIAMIMATE_COLLECTOR_ENV_FILE=$ENV_FILE
EnvironmentFile=-$ENV_FILE
ExecStart=$(exec_start_for "$job")
Nice=10
EOF
}

write_timer() {
    local job="$1"
    local timer_path
    timer_path="$(timer_path_for "$job")"

    cat > "$timer_path" <<EOF
[Unit]
Description=$(description_for "$job") Timer

[Timer]
OnCalendar=$(schedule_for "$job")
Persistent=true
Unit=$(service_name_for "$job")

[Install]
WantedBy=timers.target
EOF
}

install_job() {
    local job="$1"
    local timer_name
    timer_name="$(timer_name_for "$job")"

    write_service "$job"
    write_timer "$job"
    systemctl daemon-reload
    systemctl enable --now "$timer_name"
    echo "installed: $(service_name_for "$job") + $timer_name"
}

uninstall_job() {
    local job="$1"
    local service_name timer_name
    service_name="$(service_name_for "$job")"
    timer_name="$(timer_name_for "$job")"

    systemctl disable --now "$timer_name" >/dev/null 2>&1 || true
    systemctl stop "$service_name" >/dev/null 2>&1 || true
    rm -f "$(service_path_for "$job")" "$(timer_path_for "$job")"
    echo "uninstalled: $service_name + $timer_name"
}

status_job() {
    local job="$1"
    local service_name timer_name
    service_name="$(service_name_for "$job")"
    timer_name="$(timer_name_for "$job")"

    echo "=== $service_name ==="
    echo -n "active="
    systemctl is-active "$service_name" || true
    echo "=== $timer_name ==="
    echo -n "active="
    systemctl is-active "$timer_name" || true
    echo -n "enabled="
    systemctl is-enabled "$timer_name" || true
    systemctl list-timers "$timer_name" --no-pager || true
}

run_once_job() {
    local job="$1"
    local service_name
    service_name="$(service_name_for "$job")"
    systemctl start "$service_name"
    systemctl status "$service_name" --no-pager --lines=40 || true
}

logs_job() {
    local job="$1"
    journalctl -u "$(service_name_for "$job")" -u "$(timer_name_for "$job")" -n 80 --no-pager
}

run_for_jobs() {
    local action="$1"
    local target="${2:-all}"
    local jobs=()

    if [[ "$target" == "all" ]]; then
        jobs=(raw-cleanup)
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
            run-once)
                run_once_job "$job"
                ;;
            logs)
                logs_job "$job"
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
    bash scripts/manage_ecs2_lifecycle_services.sh install [all|raw-cleanup|duckdb-snapshot]
    bash scripts/manage_ecs2_lifecycle_services.sh uninstall [all|raw-cleanup|duckdb-snapshot]
    bash scripts/manage_ecs2_lifecycle_services.sh status [all|raw-cleanup|duckdb-snapshot]
  bash scripts/manage_ecs2_lifecycle_services.sh run-once [raw-cleanup|duckdb-snapshot]
  bash scripts/manage_ecs2_lifecycle_services.sh logs [raw-cleanup|duckdb-snapshot]

Note:
    all installs raw-cleanup only. DuckDB snapshots are refreshed by pg/theme sync jobs.
EOF
}

if ! systemd_available; then
    echo "systemd is not available on this host" >&2
    exit 1
fi

case "${1:-}" in
    install|uninstall|status)
        run_for_jobs "$1" "${2:-all}"
        ;;
    run-once|logs)
        if [[ -z "${2:-}" ]]; then
            usage >&2
            exit 1
        fi
        run_for_jobs "$1" "$2"
        ;;
    *)
        usage >&2
        exit 1
        ;;
esac
