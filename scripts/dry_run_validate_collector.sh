#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/load_collector_env.sh
source "$ROOT_DIR/scripts/load_collector_env.sh"

PYTHON_BIN="${XIAMIMATE_PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
DUCKDB_PATH="${XIAMIMATE_DUCKDB_PATH:-${DUCKDB_PATH:-}}"
RAW_PRODUCTS_DIR="${XIAMIMATE_RAW_PRODUCTS_DIR:-}"
LOG_DIR="${XIAMIMATE_LOG_DIR:-$ROOT_DIR/logs}"
INIT_SYNC_TABLES_SQL="${XIAMIMATE_INIT_SYNC_TABLES_SQL:-}"

print_python_hint() {
    cat >&2 <<EOF
Hint:
- phase 2 不需要复制 .venv 到 xiamimate-collector
- 当前脚本会优先读取 data_collector/.env；运行时共享路径会自动回落到同级旧仓 ${XIAMIMATE_BASELINE_ROOT:-../xiamimate}
- init_sync_tables.sql 会优先回落到同级 data-infra 仓 ${XIAMIMATE_DATA_INFRA_ROOT:-../xiamimate-data-infra}
- 如果你想显式指定解释器，请在 data_collector/.env 里设置 XIAMIMATE_PYTHON_BIN=/absolute/path/to/python
- 如果后面要真正独立，再单独创建新环境（venv 或 conda 都可以），然后把 XIAMIMATE_PYTHON_BIN 指到那个环境
EOF
}

require_file() {
    local path="$1"
    local label="$2"
    if [[ -z "$path" || ! -f "$path" ]]; then
        echo "[FAIL] $label not found: ${path:-<empty>}" >&2
        if [[ "$label" == "Python interpreter" ]]; then
            print_python_hint
        fi
        exit 1
    fi
    echo "[OK] $label: $path"
}

require_dir() {
    local path="$1"
    local label="$2"
    if [[ -z "$path" || ! -d "$path" ]]; then
        echo "[FAIL] $label not found: ${path:-<empty>}" >&2
        exit 1
    fi
    echo "[OK] $label: $path"
}

run_check() {
    local label="$1"
    shift
    echo "[RUN] $label"
    "$@" >/dev/null
    echo "[OK] $label"
}

require_file "$PYTHON_BIN" "Python interpreter"
require_file "$DUCKDB_PATH" "Shared DuckDB"
require_dir "$RAW_PRODUCTS_DIR" "Shared raw products directory"
require_dir "$LOG_DIR" "Shared log directory"
require_file "$INIT_SYNC_TABLES_SQL" "Sync schema SQL"

run_check "auto-collect preview" bash "$ROOT_DIR/scripts/manage_auto_collect.sh" preview
run_check "pg sync preview" bash "$ROOT_DIR/scripts/manage_pg_sync.sh" preview
run_check "theme feature sync preview" bash "$ROOT_DIR/scripts/manage_theme_feature_sync.sh" preview
run_check "collector CLI help" "$PYTHON_BIN" -m data_collector.cross_border_data --help
run_check "fetch-categories help" "$PYTHON_BIN" -m data_collector.cross_border_data fetch-categories --help
run_check "auto-collect help" "$PYTHON_BIN" -m data_collector.cross_border_data auto-collect --help
run_check "backfill-product-raw help" "$PYTHON_BIN" -m data_collector.cross_border_data backfill-product-raw --help
run_check "sync_duckdb_to_pg help" "$PYTHON_BIN" "$ROOT_DIR/data_collector/sync_duckdb_to_pg.py" --help
run_check "sync_theme_features_to_pg help" "$PYTHON_BIN" "$ROOT_DIR/data_collector/sync_theme_features_to_pg.py" --help

echo "[DONE] collector phase 2 dry-run validation passed"