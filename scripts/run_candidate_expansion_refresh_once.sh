#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/load_collector_env.sh
source "$ROOT_DIR/scripts/load_collector_env.sh"

PYTHON_BIN="${XIAMIMATE_PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
LOG_DIR="${XIAMIMATE_LOG_DIR:-$ROOT_DIR/logs}"
CANDIDATE_EXPANSION_DUCKDB_SOURCE="${CANDIDATE_EXPANSION_DUCKDB_SOURCE:-live}"
DUCKDB_ACCESS_LOCK_FILE="${XIAMIMATE_DUCKDB_ACCESS_LOCK_FILE:-$LOG_DIR/duckdb_live_access.lock}"
DUCKDB_ACCESS_LOCK_TIMEOUT_SECONDS="${CANDIDATE_EXPANSION_DUCKDB_ACCESS_LOCK_TIMEOUT_SECONDS:-${XIAMIMATE_DUCKDB_ACCESS_LOCK_TIMEOUT_SECONDS:-900}}"
mkdir -p "$LOG_DIR"

cleanup_subset_duckdb() {
    if [[ -n "${CANDIDATE_EXPANSION_SUBSET_DUCKDB_PATH:-}" && "${CANDIDATE_EXPANSION_KEEP_SUBSET_DUCKDB:-false}" != "true" ]]; then
        rm -f "$CANDIDATE_EXPANSION_SUBSET_DUCKDB_PATH" \
              "$CANDIDATE_EXPANSION_SUBSET_DUCKDB_PATH.wal" \
              "${CANDIDATE_EXPANSION_SUBSET_DUCKDB_PATH%.duckdb}.manifest.json"
    fi
}
trap cleanup_subset_duckdb EXIT

job_ids="${CANDIDATE_EXPANSION_JOB_IDS:-}"
if [[ "$#" -gt 0 ]]; then
    arg_job_ids="$(printf '%s\n' "$@" | paste -sd, -)"
    if [[ -n "$job_ids" ]]; then
        job_ids="$job_ids,$arg_job_ids"
    else
        job_ids="$arg_job_ids"
    fi
fi

job_ids="$(printf '%s' "$job_ids" | tr ' ' ',' | awk -F, '{for (i=1;i<=NF;i++) if ($i != "") print $i}' | sort -u | paste -sd, -)"
if [[ -z "$job_ids" ]]; then
    echo "candidate expansion refresh: no job ids supplied"
    exit 0
fi

export CANDIDATE_EXPANSION_JOB_IDS="$job_ids"

case "$CANDIDATE_EXPANSION_DUCKDB_SOURCE" in
    live)
        LIVE_DUCKDB_PATH="${CANDIDATE_EXPANSION_LIVE_DUCKDB_PATH:-${XIAMIMATE_DUCKDB_PATH:-}}"
        if [[ -z "$LIVE_DUCKDB_PATH" || ! -f "$LIVE_DUCKDB_PATH" ]]; then
            echo "candidate expansion refresh: live DuckDB not found: ${LIVE_DUCKDB_PATH:-<empty>}" >&2
            exit 1
        fi
        subset_dir="${CANDIDATE_EXPANSION_DUCKDB_SUBSET_DIR:-$LOG_DIR/candidate_expansion_duckdb_subsets}"
        mkdir -p "$(dirname "$DUCKDB_ACCESS_LOCK_FILE")"
        {
            if ! flock -x -w "$DUCKDB_ACCESS_LOCK_TIMEOUT_SECONDS" 8; then
                echo "candidate expansion refresh: timed out waiting for DuckDB access lock: $DUCKDB_ACCESS_LOCK_FILE" >&2
                exit 1
            fi
            printf 'pid=%s\nrole=candidate_expansion_subset\nacquired_at=%s\n' "$$" "$(date -u +%FT%TZ)" >"$DUCKDB_ACCESS_LOCK_FILE"
            eval "$("$PYTHON_BIN" "$ROOT_DIR/scripts/build_candidate_expansion_duckdb_subset.py" \
                --source-db "$LIVE_DUCKDB_PATH" \
                --output-dir "$subset_dir" \
                --job-ids "$job_ids" \
                --emit-shell)"
            : >"$DUCKDB_ACCESS_LOCK_FILE"
            flock -u 8
        } 8>"$DUCKDB_ACCESS_LOCK_FILE"
        if [[ -z "${CANDIDATE_EXPANSION_SUBSET_DUCKDB_PATH:-}" ]]; then
            echo "candidate expansion refresh: no ASINs found for job ids: $job_ids"
            exit 0
        fi
        export PG_SYNC_DUCKDB_PATH="$CANDIDATE_EXPANSION_SUBSET_DUCKDB_PATH"
        export THEME_FEATURE_SYNC_DUCKDB_PATH="$CANDIDATE_EXPANSION_SUBSET_DUCKDB_PATH"
        export PG_SYNC_DUCKDB_READ_MODE=direct
        export THEME_FEATURE_DUCKDB_READ_MODE=direct
        export DUCKDB_OPEN_RETRIES="${DUCKDB_OPEN_RETRIES:-${CANDIDATE_EXPANSION_DUCKDB_OPEN_RETRIES:-30}}"
        export DUCKDB_OPEN_RETRY_DELAY_SECONDS="${DUCKDB_OPEN_RETRY_DELAY_SECONDS:-${CANDIDATE_EXPANSION_DUCKDB_OPEN_RETRY_DELAY_SECONDS:-2}}"
        export CANDIDATE_EXPANSION_REFRESH_SNAPSHOT="${CANDIDATE_EXPANSION_REFRESH_SNAPSHOT:-false}"
        ;;
    snapshot)
        export CANDIDATE_EXPANSION_REFRESH_SNAPSHOT="${CANDIDATE_EXPANSION_REFRESH_SNAPSHOT:-true}"
        ;;
    *)
        echo "candidate expansion refresh: unsupported CANDIDATE_EXPANSION_DUCKDB_SOURCE=$CANDIDATE_EXPANSION_DUCKDB_SOURCE (expected live|snapshot)" >&2
        exit 1
        ;;
esac

export PG_SYNC_TABLES="${CANDIDATE_EXPANSION_PG_SYNC_TABLES:-curated.keepa_asin_registry,curated.keepa_product_snapshot,curated.keepa_product_history,curated.asin_keyword_mapping,curated.asin_raw_file_mapping,curated.discovery_expansion_state}"
export PG_SYNC_SKIP_TABLES=""
export PG_SYNC_REFRESH_SNAPSHOT="${CANDIDATE_EXPANSION_REFRESH_SNAPSHOT:-false}"
export PG_SYNC_TRIGGER_THEME_SYNC_ON_EXPANSION_RECONCILE=false

if [[ "${CANDIDATE_EXPANSION_RUN_PG_SYNC:-true}" == "true" ]]; then
    /bin/bash "$ROOT_DIR/scripts/run_pg_sync_once.sh"
fi

if [[ -n "${CANDIDATE_EXPANSION_TARGET_ASINS:-}" && -n "${CANDIDATE_EXPANSION_TARGET_DOMAINS:-}" ]]; then
    asins="$CANDIDATE_EXPANSION_TARGET_ASINS"
    domains="$CANDIDATE_EXPANSION_TARGET_DOMAINS"
else
    mapfile -t job_rows < <("$PYTHON_BIN" - <<'PY'
import os
import psycopg2

job_ids = [item.strip() for item in os.environ.get("CANDIDATE_EXPANSION_JOB_IDS", "").split(",") if item.strip()]
if not job_ids:
    raise SystemExit(0)

conn = psycopg2.connect(
    host=os.environ.get("PG_HOST"),
    port=int(os.environ.get("PG_PORT", "5432")),
    dbname=os.environ.get("PG_DB"),
    user=os.environ.get("PG_USER"),
    password=os.environ.get("PG_PASSWORD"),
)
try:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT j.domain, asin
            FROM sync.keepa_candidate_expansion_jobs j
            CROSS JOIN LATERAL unnest(COALESCE(j.result_candidate_asins, ARRAY[]::TEXT[])) AS asin
            WHERE j.job_id = ANY(%s)
            ORDER BY j.domain, asin
        """, [job_ids])
        for domain, asin in cur.fetchall():
            print(f"{domain}\t{asin}")
finally:
    conn.close()
PY
    )

    if [[ "${#job_rows[@]}" -eq 0 ]]; then
        echo "candidate expansion refresh: no ASINs found for job ids: $job_ids"
        exit 0
    fi

    asins="$(printf '%s\n' "${job_rows[@]}" | awk -F '\t' '{print $2}' | sort -u | paste -sd, -)"
    domains="$(printf '%s\n' "${job_rows[@]}" | awk -F '\t' '{print $1}' | sort -un | paste -sd, -)"
fi

export THEME_FEATURE_TARGET_ASINS="$asins"
export THEME_FEATURE_TARGET_DOMAINS="$domains"
export THEME_FEATURE_SYNC_REFRESH_SNAPSHOT=false
export THEME_FEATURE_REFRESH_OVERLAP_DAYS="${CANDIDATE_EXPANSION_THEME_OVERLAP_DAYS:-7}"

/bin/bash "$ROOT_DIR/scripts/run_theme_feature_sync_once.sh"
