#!/usr/bin/env bash

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COLLECTOR_ENV_FILE="${XIAMIMATE_COLLECTOR_ENV_FILE:-$ROOT_DIR/data_collector/.env}"

set_default_if_missing() {
    local var_name="$1"
    local candidate="$2"

    if [[ -n "${!var_name:-}" ]]; then
        return 0
    fi
    if [[ -z "$candidate" ]]; then
        return 0
    fi

    printf -v "$var_name" '%s' "$candidate"
    export "$var_name"
}

if [[ -f "$COLLECTOR_ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$COLLECTOR_ENV_FILE"
    set +a
fi

if [[ -z "${XIAMIMATE_RUNTIME_ROOT:-}" ]]; then
    default_runtime_root="$(cd "$ROOT_DIR/../xiamimate-runtime" 2>/dev/null && pwd || true)"
    if [[ -n "$default_runtime_root" && -d "$default_runtime_root" ]]; then
        XIAMIMATE_RUNTIME_ROOT="$default_runtime_root"
    fi
fi

if [[ -z "${XIAMIMATE_DATA_INFRA_ROOT:-}" ]]; then
    default_data_infra_root="$(cd "$ROOT_DIR/../xiamimate-data-infra" 2>/dev/null && pwd || true)"
    if [[ -n "$default_data_infra_root" && -d "$default_data_infra_root" ]]; then
        XIAMIMATE_DATA_INFRA_ROOT="$default_data_infra_root"
    fi
fi

if [[ -z "${XIAMIMATE_BASELINE_ROOT:-}" ]]; then
    default_baseline_root="$(cd "$ROOT_DIR/../xiamimate" 2>/dev/null && pwd || true)"
    if [[ -n "$default_baseline_root" && -d "$default_baseline_root" ]]; then
        XIAMIMATE_BASELINE_ROOT="$default_baseline_root"
    fi
fi

if [[ -n "${XIAMIMATE_DATA_INFRA_ROOT:-}" ]]; then
    data_infra_sync_sql="$XIAMIMATE_DATA_INFRA_ROOT/postgres/init_sync_tables.sql"
    if [[ -f "$data_infra_sync_sql" ]]; then
        set_default_if_missing "XIAMIMATE_INIT_SYNC_TABLES_SQL" "$data_infra_sync_sql"
    fi
fi

if [[ -n "${XIAMIMATE_RUNTIME_ROOT:-}" ]]; then
    set_default_if_missing "XIAMIMATE_PYTHON_BIN" "$XIAMIMATE_RUNTIME_ROOT/python/.venv/bin/python"
    set_default_if_missing "XIAMIMATE_DUCKDB_PATH" "$XIAMIMATE_RUNTIME_ROOT/duckdb/warehouse/local_analytics.duckdb"
    set_default_if_missing "XIAMIMATE_RAW_PRODUCTS_DIR" "$XIAMIMATE_RUNTIME_ROOT/raw/json/products"
fi

if [[ -n "${XIAMIMATE_BASELINE_ROOT:-}" ]]; then
    set_default_if_missing "XIAMIMATE_PYTHON_BIN" "$XIAMIMATE_BASELINE_ROOT/.venv/bin/python"
    set_default_if_missing "XIAMIMATE_DUCKDB_PATH" "$XIAMIMATE_BASELINE_ROOT/data_platform/storage/warehouse/local_analytics.duckdb"
    set_default_if_missing "XIAMIMATE_RAW_PRODUCTS_DIR" "$XIAMIMATE_BASELINE_ROOT/data_platform/storage/raw/json/products"
    set_default_if_missing "XIAMIMATE_INIT_SYNC_TABLES_SQL" "$XIAMIMATE_BASELINE_ROOT/data_platform/postgres/init_sync_tables.sql"
fi

if [[ -z "${XIAMIMATE_LOG_DIR:-}" ]]; then
    XIAMIMATE_LOG_DIR="$ROOT_DIR/logs"
fi

export XIAMIMATE_RUNTIME_ROOT
export XIAMIMATE_DATA_INFRA_ROOT
export XIAMIMATE_BASELINE_ROOT
export XIAMIMATE_LOG_DIR