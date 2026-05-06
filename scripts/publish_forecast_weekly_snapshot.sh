#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/load_collector_env.sh
source "$ROOT_DIR/scripts/load_collector_env.sh"

PYTHON_BIN="${XIAMIMATE_PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
FORECAST_SNAPSHOT_MODE="${FORECAST_SNAPSHOT_MODE:-feature_parquet}"

case "$FORECAST_SNAPSHOT_MODE" in
  feature_parquet)
    exec "$PYTHON_BIN" "$ROOT_DIR/scripts/export_forecast_feature_partitions.py"
    ;;
  duckdb)
    exec bash "$ROOT_DIR/scripts/publish_forecast_duckdb_snapshot.sh"
    ;;
  *)
    echo "unsupported FORECAST_SNAPSHOT_MODE=$FORECAST_SNAPSHOT_MODE (expected feature_parquet|duckdb)" >&2
    exit 1
    ;;
esac
