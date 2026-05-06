#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/load_collector_env.sh
source "$ROOT_DIR/scripts/load_collector_env.sh"

export PG_SYNC_TABLES="${PG_SYNC_HISTORY_TABLES:-curated.keepa_product_history}"
export PG_SYNC_SKIP_TABLES=""
export PG_SYNC_REFRESH_SNAPSHOT="${PG_SYNC_HISTORY_REFRESH_SNAPSHOT:-false}"

/bin/bash "$ROOT_DIR/scripts/run_pg_sync_once.sh"
