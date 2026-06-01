#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${XIAMIMATE_COLLECTOR_ENV_FILE:-$ROOT_DIR/data_collector/.env}"
SYSTEMD_DIR="${XIAMIMATE_COLLECTOR_SYSTEMD_DIR:-/etc/systemd/system}"
DEFAULT_LOG_DIR="/data/xiamimate/collector/logs"

if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
fi

PG_INTERVAL="${XIAMIMATE_PG_SYNC_TIMER_INTERVAL:-5min}"
THEME_CALENDAR="${XIAMIMATE_THEME_SYNC_TIMER_CALENDAR:-*-*-* 01:00:00}"
PG_AGG_CALENDAR="${XIAMIMATE_PG_AGG_SYNC_TIMER_CALENDAR:-Sun *-*-* 04:00:00}"
DEFAULT_TIMEOUT_START_SEC="${XIAMIMATE_SYNC_TIMER_TIMEOUT_START_SEC:-45min}"
PG_AGG_TIMEOUT_START_SEC="${XIAMIMATE_PG_AGG_SYNC_TIMEOUT_START_SEC:-3h}"

systemd_available() {
    command -v systemctl >/dev/null 2>&1
}

service_name_for() {
    case "$1" in
        pg-sync)
            echo "xiamimate-pg-sync-snapshot.service"
            ;;
        theme-sync)
            echo "xiamimate-theme-sync-snapshot.service"
            ;;
        pg-agg-sync)
            echo "xiamimate-pg-agg-sync-snapshot.service"
            ;;
        *)
            echo "unsupported job: $1" >&2
            return 1
            ;;
    esac
}

timer_name_for() {
    case "$1" in
        pg-sync)
            echo "xiamimate-pg-sync-snapshot.timer"
            ;;
        theme-sync)
            echo "xiamimate-theme-sync-snapshot.timer"
            ;;
        pg-agg-sync)
            echo "xiamimate-pg-agg-sync-snapshot.timer"
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
        pg-sync)
            echo "XiaMimate PostgreSQL Live DuckDB Sync"
            ;;
        theme-sync)
            echo "XiaMimate Theme Snapshot Sync"
            ;;
        pg-agg-sync)
            echo "XiaMimate PostgreSQL Aggregate Snapshot Sync"
            ;;
    esac
}

exec_start_for() {
    case "$1" in
        pg-sync)
            echo "/bin/bash $ROOT_DIR/scripts/run_pg_sync_once.sh"
            ;;
        theme-sync)
            echo "/bin/bash $ROOT_DIR/scripts/run_theme_feature_sync_once.sh"
            ;;
        pg-agg-sync)
            echo "/bin/bash $ROOT_DIR/scripts/run_pg_sync_agg_once.sh"
            ;;
    esac
}

log_file_for() {
    case "$1" in
        pg-sync)
            echo "$DEFAULT_LOG_DIR/pg_sync.timer.log"
            ;;
        theme-sync)
            echo "$DEFAULT_LOG_DIR/theme_sync.timer.log"
            ;;
        pg-agg-sync)
            echo "$DEFAULT_LOG_DIR/pg_agg_sync.timer.log"
            ;;
    esac
}

timeout_start_sec_for() {
    case "$1" in
        pg-agg-sync)
            echo "$PG_AGG_TIMEOUT_START_SEC"
            ;;
        *)
            echo "$DEFAULT_TIMEOUT_START_SEC"
            ;;
    esac
}

write_service() {
    local job="$1"
    local service_path
    service_path="$(service_path_for "$job")"

    cat > "$service_path" <<EOF_UNIT
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
ExecStartPre=/bin/mkdir -p $DEFAULT_LOG_DIR
ExecStart=$(exec_start_for "$job")
TimeoutStartSec=$(timeout_start_sec_for "$job")
Nice=10
IOSchedulingClass=best-effort
IOSchedulingPriority=7
StandardOutput=append:$(log_file_for "$job")
StandardError=append:$(log_file_for "$job")
EOF_UNIT
}

write_timer() {
    local job="$1"
    local timer_path
    timer_path="$(timer_path_for "$job")"

    case "$job" in
        pg-sync)
            cat > "$timer_path" <<EOF_TIMER
[Unit]
Description=$(description_for "$job") Timer

[Timer]
OnActiveSec=$PG_INTERVAL
OnUnitActiveSec=$PG_INTERVAL
AccuracySec=30s
Persistent=false
Unit=$(service_name_for "$job")

[Install]
WantedBy=timers.target
EOF_TIMER
            ;;
        theme-sync)
            cat > "$timer_path" <<EOF_TIMER
[Unit]
Description=$(description_for "$job") Timer

[Timer]
OnCalendar=$THEME_CALENDAR
Persistent=true
Unit=$(service_name_for "$job")

[Install]
WantedBy=timers.target
EOF_TIMER
            ;;
        pg-agg-sync)
            cat > "$timer_path" <<EOF_TIMER
[Unit]
Description=$(description_for "$job") Timer

[Timer]
OnCalendar=$PG_AGG_CALENDAR
Persistent=true
Unit=$(service_name_for "$job")

[Install]
WantedBy=timers.target
EOF_TIMER
            ;;
    esac
}

disable_old_loop_service() {
    local old_unit=""
    case "$1" in
        pg-sync)
            old_unit="xiamimate-pg-sync.service xiamimate-pg-sync-window.service xiamimate-pg-sync-window.timer"
            ;;
        theme-sync)
            old_unit="xiamimate-theme-sync.service xiamimate-theme-sync-window.service xiamimate-theme-sync-window.timer"
            ;;
    esac

    if [[ -n "$old_unit" ]]; then
        # shellcheck disable=SC2086
        systemctl disable --now $old_unit >/dev/null 2>&1 || true
    fi
}

install_job() {
    local job="$1"
    local timer_name
    timer_name="$(timer_name_for "$job")"

    disable_old_loop_service "$job"
    write_service "$job"
    write_timer "$job"
    systemctl daemon-reload
    systemctl enable --now "$timer_name"
    echo "installed: $(service_name_for "$job") + $timer_name"
}

decommission_removed_jobs() {
    # pg-history-sync 已并入 pg-sync。在仍有残留 unit 的主机上把 legacy timer 清掉，
    # 然后通过 case 表移除该 job 名后，这里直接按 unit 名操作。
    local legacy_units=(
        "xiamimate-pg-history-sync-snapshot.timer"
        "xiamimate-pg-history-sync-snapshot.service"
    )
    for unit in "${legacy_units[@]}"; do
        systemctl disable --now "$unit" >/dev/null 2>&1 || true
        rm -f "$SYSTEMD_DIR/$unit"
    done
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
    systemctl status "$service_name" --no-pager --lines=60 || true
}

logs_job() {
    local job="$1"
    journalctl -u "$(service_name_for "$job")" -u "$(timer_name_for "$job")" -n 100 --no-pager
}

run_for_jobs() {
    local action="$1"
    local target="${2:-all}"
    local jobs=()

    if [[ "$target" == "all" ]]; then
        case "$action" in
            install|status|logs|run-once)
                jobs=(pg-sync theme-sync pg-agg-sync)
                ;;
            uninstall)
                jobs=(pg-sync theme-sync pg-agg-sync)
                ;;
        esac
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
        if [[ "$action" == "install" && "$target" == "all" ]]; then
            decommission_removed_jobs
        fi
        systemctl daemon-reload
        systemctl reset-failed >/dev/null 2>&1 || true
    fi
}

usage() {
    cat <<EOF_USAGE
Usage:
    bash scripts/manage_ecs2_sync_timers.sh install [all|pg-sync|theme-sync|pg-agg-sync]
    bash scripts/manage_ecs2_sync_timers.sh uninstall [all|pg-sync|theme-sync|pg-agg-sync]
    bash scripts/manage_ecs2_sync_timers.sh status [all|pg-sync|theme-sync|pg-agg-sync]
    bash scripts/manage_ecs2_sync_timers.sh run-once [pg-sync|theme-sync|pg-agg-sync]
    bash scripts/manage_ecs2_sync_timers.sh logs [pg-sync|theme-sync|pg-agg-sync]

Environment:
  XIAMIMATE_COLLECTOR_ENV_FILE         default: $ROOT_DIR/data_collector/.env
  XIAMIMATE_COLLECTOR_SYSTEMD_DIR      default: /etc/systemd/system
    XIAMIMATE_PG_SYNC_TIMER_INTERVAL     default: 5min
    XIAMIMATE_THEME_SYNC_TIMER_CALENDAR  default: *-*-* 01:00:00
    XIAMIMATE_PG_AGG_SYNC_TIMER_CALENDAR default: Sun *-*-* 04:00:00
    legacy pg-history-sync timer is auto-removed by 'install all' (history is merged into pg-sync).
EOF_USAGE
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
