#!/usr/bin/env bash

set -euo pipefail

export PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/load_collector_env.sh
source "$ROOT_DIR/scripts/load_collector_env.sh"

PYTHON_BIN="${XIAMIMATE_COLLECTOR_HEALTHCHECK_PYTHON_BIN:-${XIAMIMATE_PYTHON_BIN:-$(command -v python3 || true)}}"
LOG_DIR="${XIAMIMATE_LOG_DIR:-/data/xiamimate/collector/logs}"
HEALTHCHECK_SCRIPT="$ROOT_DIR/scripts/check_ecs2_collector_health.sh"
NOTIFIER_SCRIPT="$ROOT_DIR/scripts/collector_healthcheck_notifier.py"
STATE_FILE="${XIAMIMATE_COLLECTOR_HEALTHCHECK_NOTIFY_STATE_FILE:-$LOG_DIR/collector_healthcheck_notify_state.json}"
COOLDOWN_SECONDS="${XIAMIMATE_COLLECTOR_HEALTHCHECK_NOTIFY_COOLDOWN_SECONDS:-3600}"
NOTIFY_RECOVERY="${XIAMIMATE_COLLECTOR_HEALTHCHECK_NOTIFY_RECOVERY:-1}"
NOTIFY_ENABLED="${XIAMIMATE_COLLECTOR_HEALTHCHECK_NOTIFY_ENABLED:-true}"

mkdir -p "$LOG_DIR"

tmp_output="$(mktemp)"
cleanup() {
    rm -f "$tmp_output"
}
trap cleanup EXIT

healthcheck_exit=0
set +e
/bin/bash "$HEALTHCHECK_SCRIPT" > "$tmp_output" 2>&1
healthcheck_exit=$?
set -e

cat "$tmp_output"

if [[ "$NOTIFY_ENABLED" == "true" ]]; then
    if [[ -z "$PYTHON_BIN" || ! -x "$PYTHON_BIN" ]]; then
        echo "healthcheck notifier: python not found: $PYTHON_BIN" >&2
    else
        notify_args=(
            "$PYTHON_BIN"
            "$NOTIFIER_SCRIPT"
            --healthcheck-output-file "$tmp_output"
            --healthcheck-exit-code "$healthcheck_exit"
            --state-file "$STATE_FILE"
            --cooldown-seconds "$COOLDOWN_SECONDS"
        )
        if [[ -n "${FEISHU_WEBHOOK_URL:-}" ]]; then
            notify_args+=(--webhook-url "$FEISHU_WEBHOOK_URL")
        fi
        if [[ "$NOTIFY_RECOVERY" == "1" ]]; then
            notify_args+=(--notify-recovery)
        fi
        "${notify_args[@]}" || echo "healthcheck notifier failed" >&2
    fi
fi

exit "$healthcheck_exit"
