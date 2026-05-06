#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${XIAMIMATE_COLLECTOR_ENV_FILE:-$ROOT_DIR/data_collector/.env}"
SYSTEMD_DIR="${XIAMIMATE_COLLECTOR_SYSTEMD_DIR:-/etc/systemd/system}"
DEFAULT_LOG_DIR="/data/xiamimate/collector/logs"
INTERVAL="${XIAMIMATE_COLLECTOR_HEALTHCHECK_INTERVAL:-5min}"
SERVICE_NAME="xiamimate-collector-healthcheck.service"
TIMER_NAME="xiamimate-collector-healthcheck.timer"

if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
    INTERVAL="${XIAMIMATE_COLLECTOR_HEALTHCHECK_INTERVAL:-$INTERVAL}"
fi

systemd_available() {
    command -v systemctl >/dev/null 2>&1
}

service_path() {
    echo "$SYSTEMD_DIR/$SERVICE_NAME"
}

timer_path() {
    echo "$SYSTEMD_DIR/$TIMER_NAME"
}

write_service() {
    cat > "$(service_path)" <<EOF_UNIT
[Unit]
Description=XiaMimate Collector Healthcheck
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
ExecStart=/bin/bash $ROOT_DIR/scripts/run_ecs2_collector_healthcheck_and_notify.sh
TimeoutStartSec=5min
Nice=10
IOSchedulingClass=best-effort
IOSchedulingPriority=7
StandardOutput=append:$DEFAULT_LOG_DIR/collector_healthcheck.timer.log
StandardError=append:$DEFAULT_LOG_DIR/collector_healthcheck.timer.log
EOF_UNIT
}

write_timer() {
    cat > "$(timer_path)" <<EOF_TIMER
[Unit]
Description=XiaMimate Collector Healthcheck Timer

[Timer]
OnBootSec=2min
OnUnitActiveSec=$INTERVAL
AccuracySec=30s
Persistent=false
Unit=$SERVICE_NAME

[Install]
WantedBy=timers.target
EOF_TIMER
}

install_monitoring() {
    write_service
    write_timer
    systemctl daemon-reload
    systemctl enable --now "$TIMER_NAME"
    echo "installed: $SERVICE_NAME + $TIMER_NAME"
}

uninstall_monitoring() {
    systemctl disable --now "$TIMER_NAME" >/dev/null 2>&1 || true
    systemctl stop "$SERVICE_NAME" >/dev/null 2>&1 || true
    rm -f "$(service_path)" "$(timer_path)"
    systemctl daemon-reload
    systemctl reset-failed >/dev/null 2>&1 || true
    echo "uninstalled: $SERVICE_NAME + $TIMER_NAME"
}

status_monitoring() {
    echo "=== $SERVICE_NAME ==="
    echo -n "active="
    systemctl is-active "$SERVICE_NAME" || true
    echo -n "failed="
    systemctl is-failed "$SERVICE_NAME" || true
    echo "=== $TIMER_NAME ==="
    echo -n "active="
    systemctl is-active "$TIMER_NAME" || true
    echo -n "enabled="
    systemctl is-enabled "$TIMER_NAME" || true
    systemctl list-timers "$TIMER_NAME" --no-pager || true
}

run_once_monitoring() {
    systemctl start "$SERVICE_NAME"
    systemctl status "$SERVICE_NAME" --no-pager --lines=80 || true
}

logs_monitoring() {
    journalctl -u "$SERVICE_NAME" -u "$TIMER_NAME" -n 120 --no-pager || true
    if [[ -f "$DEFAULT_LOG_DIR/collector_healthcheck.timer.log" ]]; then
        echo "=== $DEFAULT_LOG_DIR/collector_healthcheck.timer.log ==="
        tail -n 120 "$DEFAULT_LOG_DIR/collector_healthcheck.timer.log" || true
    fi
}

usage() {
    cat <<EOF_USAGE
Usage:
  bash scripts/manage_ecs2_monitoring.sh install
  bash scripts/manage_ecs2_monitoring.sh uninstall
  bash scripts/manage_ecs2_monitoring.sh status
  bash scripts/manage_ecs2_monitoring.sh run-once
  bash scripts/manage_ecs2_monitoring.sh logs

Environment:
  XIAMIMATE_COLLECTOR_ENV_FILE              default: $ROOT_DIR/data_collector/.env
  XIAMIMATE_COLLECTOR_SYSTEMD_DIR           default: /etc/systemd/system
  XIAMIMATE_COLLECTOR_HEALTHCHECK_INTERVAL  default: 5min
    XIAMIMATE_COLLECTOR_HEALTHCHECK_NOTIFY_ENABLED default: true
    XIAMIMATE_COLLECTOR_HEALTHCHECK_NOTIFY_COOLDOWN_SECONDS default: 3600
EOF_USAGE
}

if ! systemd_available; then
    echo "systemd is not available on this host" >&2
    exit 1
fi

case "${1:-}" in
    install)
        install_monitoring
        ;;
    uninstall)
        uninstall_monitoring
        ;;
    status)
        status_monitoring
        ;;
    run-once)
        run_once_monitoring
        ;;
    logs)
        logs_monitoring
        ;;
    *)
        usage >&2
        exit 1
        ;;
esac
