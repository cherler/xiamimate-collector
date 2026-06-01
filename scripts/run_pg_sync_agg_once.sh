#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/load_collector_env.sh
source "$ROOT_DIR/scripts/load_collector_env.sh"

export PG_SYNC_TABLES="${PG_SYNC_AGG_TABLES:-agg.keepa_history_domain_daily,agg.keepa_history_root_category_daily}"
export PG_SYNC_SKIP_TABLES=""
export PG_SYNC_REFRESH_SNAPSHOT="${PG_SYNC_AGG_REFRESH_SNAPSHOT:-false}"
export PG_SYNC_DUCKDB_SOURCE="${PG_SYNC_AGG_DUCKDB_SOURCE:-snapshot}"
export PG_SYNC_INCLUDE_HISTORY="false"
export PG_SYNC_DUCKDB_THREADS="${PG_SYNC_AGG_DUCKDB_THREADS:-1}"
export PG_SYNC_DUCKDB_MEMORY_LIMIT="${PG_SYNC_AGG_DUCKDB_MEMORY_LIMIT:-${PG_SYNC_DUCKDB_MEMORY_LIMIT:-4096MB}}"
export PG_SYNC_AGG_PARTITIONED="${PG_SYNC_AGG_PARTITIONED:-true}"
export PG_SYNC_PROGRESS_LOG_FETCH_BATCHES="${PG_SYNC_AGG_PROGRESS_LOG_FETCH_BATCHES:-${PG_SYNC_PROGRESS_LOG_FETCH_BATCHES:-100}}"

/bin/bash "$ROOT_DIR/scripts/run_pg_sync_once.sh"
