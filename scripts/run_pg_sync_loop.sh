#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/load_collector_env.sh
source "$ROOT_DIR/scripts/load_collector_env.sh"

INTERVAL="${SYNC_INTERVAL_SECONDS:-300}"

cd "$ROOT_DIR"

while true; do
    started_at="$(date '+%F %T %z')"
    start_epoch="$(date +%s)"
    echo "[$started_at] [pg-sync-loop] run start"
    /bin/bash "$ROOT_DIR/scripts/run_pg_sync_once.sh"
    end_epoch="$(date +%s)"
    echo "[$(date '+%F %T %z')] [pg-sync-loop] run finished elapsed_sec=$((end_epoch-start_epoch)) sleep_sec=${INTERVAL}"
    sleep "$INTERVAL"
done