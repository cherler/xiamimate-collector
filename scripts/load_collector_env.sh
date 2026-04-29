#!/usr/bin/env bash

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COLLECTOR_ENV_FILE="${XIAMIMATE_COLLECTOR_ENV_FILE:-$ROOT_DIR/data_collector/.env}"

collector_is_truthy() {
    case "${1:-}" in
        1|true|TRUE|True|yes|YES|Yes|on|ON|On)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

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

if collector_is_truthy "${XIAMIMATE_COLLECTOR_FORCE_LOCAL_PG:-}"; then
    PG_HOST="${XIAMIMATE_COLLECTOR_LOCAL_PG_HOST:-localhost}"
    PG_PORT="${XIAMIMATE_COLLECTOR_LOCAL_PG_PORT:-5432}"
    PG_DB="${XIAMIMATE_COLLECTOR_LOCAL_PG_DB:-xiamimate}"
    PG_USER="${XIAMIMATE_COLLECTOR_LOCAL_PG_USER:-xiamimate}"
    PG_PASSWORD="${XIAMIMATE_COLLECTOR_LOCAL_PG_PASSWORD:-xiamimate}"
    PGPASSWORD="$PG_PASSWORD"
    PG_TUNNEL_ENABLED=0
    unset PG_TUNNEL_SSH_HOST
    unset PG_TUNNEL_REMOTE_HOST
    unset PG_TUNNEL_REMOTE_PORT
fi

if [[ -z "${XIAMIMATE_RUNTIME_ROOT:-}" ]]; then
    default_runtime_root="$(cd "$ROOT_DIR/../xiamimate-runtime" 2>/dev/null && pwd || true)"
    if [[ -n "$default_runtime_root" && -d "$default_runtime_root" ]]; then
        XIAMIMATE_RUNTIME_ROOT="$default_runtime_root"
    fi
fi

set_default_if_missing "XIAMIMATE_DATA_PLATFORM_ROOT" "/Volumes/E/data/xiamimate-data-platform"

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

set_default_if_missing "PG_PORT" "5432"

if collector_is_truthy "${PG_TUNNEL_ENABLED:-}"; then
    set_default_if_missing "PG_TUNNEL_LOCAL_HOST" "127.0.0.1"
    set_default_if_missing "PG_TUNNEL_LOCAL_PORT" "15432"

    if [[ -z "${PG_TUNNEL_REMOTE_HOST:-}" && -n "${PG_HOST:-}" ]]; then
        PG_TUNNEL_REMOTE_HOST="$PG_HOST"
    fi

    if [[ -z "${PG_TUNNEL_REMOTE_PORT:-}" && -n "${PG_PORT:-}" ]]; then
        PG_TUNNEL_REMOTE_PORT="$PG_PORT"
    fi

    PG_HOST="$PG_TUNNEL_LOCAL_HOST"
    PG_PORT="$PG_TUNNEL_LOCAL_PORT"
fi

if [[ -z "${PG_PASSWORD:-}" && -n "${PGPASSWORD:-}" ]]; then
    PG_PASSWORD="$PGPASSWORD"
fi

if [[ -z "${PGPASSWORD:-}" && -n "${PG_PASSWORD:-}" ]]; then
    PGPASSWORD="$PG_PASSWORD"
fi

collector_require_pg_env() {
    local missing=()

    if [[ -z "${PG_HOST:-}" ]]; then
        missing+=("PG_HOST")
    fi
    if [[ -z "${PG_DB:-}" ]]; then
        missing+=("PG_DB")
    fi
    if [[ -z "${PG_USER:-}" ]]; then
        missing+=("PG_USER")
    fi
    if [[ -z "${PG_PASSWORD:-}" ]]; then
        missing+=("PG_PASSWORD")
    fi

    if (( ${#missing[@]} > 0 )); then
        echo "missing collector PostgreSQL target config: ${missing[*]}" >&2
        echo "fill data_collector/.env before starting sync jobs" >&2
        return 1
    fi

    return 0
}

collector_pg_tunnel_enabled() {
    collector_is_truthy "${PG_TUNNEL_ENABLED:-}"
}

collector_pg_tunnel_pid_file() {
    echo "${PG_TUNNEL_PID_FILE:-$XIAMIMATE_LOG_DIR/pg_ssh_tunnel.pid}"
}

collector_pg_tunnel_log_file() {
    echo "${PG_TUNNEL_LOG_FILE:-$XIAMIMATE_LOG_DIR/pg_ssh_tunnel.log}"
}

collector_require_pg_tunnel_env() {
    local missing=()

    if ! collector_pg_tunnel_enabled; then
        return 0
    fi

    if [[ -z "${PG_TUNNEL_SSH_HOST:-}" ]]; then
        missing+=("PG_TUNNEL_SSH_HOST")
    fi
    if [[ -z "${PG_TUNNEL_REMOTE_HOST:-}" ]]; then
        missing+=("PG_TUNNEL_REMOTE_HOST")
    fi
    if [[ -z "${PG_TUNNEL_REMOTE_PORT:-}" ]]; then
        missing+=("PG_TUNNEL_REMOTE_PORT")
    fi

    if (( ${#missing[@]} > 0 )); then
        echo "missing collector PostgreSQL SSH tunnel config: ${missing[*]}" >&2
        echo "fill data_collector/.env before starting sync jobs with PG_TUNNEL_ENABLED=1" >&2
        return 1
    fi

    return 0
}

collector_pg_tunnel_summary() {
    local local_host="${PG_TUNNEL_LOCAL_HOST:-127.0.0.1}"
    local local_port="${PG_TUNNEL_LOCAL_PORT:-15432}"
    local ssh_host="${PG_TUNNEL_SSH_HOST:-<unset>}"
    local remote_host="${PG_TUNNEL_REMOTE_HOST:-<unset>}"
    local remote_port="${PG_TUNNEL_REMOTE_PORT:-<unset>}"

    echo "${local_host}:${local_port} via ${ssh_host} -> ${remote_host}:${remote_port}"
}

collector_pg_tunnel_resolve_pid() {
    local pid_file
    local pid
    local local_port

    pid_file="$(collector_pg_tunnel_pid_file)"
    if [[ -f "$pid_file" ]]; then
        pid="$(cat "$pid_file")"
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            echo "$pid"
            return 0
        fi
    fi

    local_port="${PG_TUNNEL_LOCAL_PORT:-15432}"
    pid="$(lsof -tiTCP:"$local_port" -sTCP:LISTEN 2>/dev/null | head -n 1 || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        echo "$pid" > "$pid_file"
        echo "$pid"
        return 0
    fi

    return 1
}

collector_pg_tunnel_is_running() {
    collector_pg_tunnel_resolve_pid >/dev/null 2>&1
}

collector_pg_target_summary() {
    local host="${PG_HOST:-<unset>}"
    local port="${PG_PORT:-5432}"
    local db="${PG_DB:-<unset>}"
    local user="${PG_USER:-<unset>}"

    if collector_pg_tunnel_enabled; then
        echo "${host}:${port}/${db} as ${user} (ssh tunnel -> ${PG_TUNNEL_REMOTE_HOST:-<unset>}:${PG_TUNNEL_REMOTE_PORT:-<unset>} via ${PG_TUNNEL_SSH_HOST:-<unset>})"
    else
        echo "${host}:${port}/${db} as ${user}"
    fi
}

collector_is_local_host() {
    case "${1:-}" in
        localhost|127.0.0.1|::1)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

collector_is_local_pg_target() {
    collector_is_local_host "${PG_HOST:-}"
}

export XIAMIMATE_RUNTIME_ROOT
export XIAMIMATE_DATA_INFRA_ROOT
export XIAMIMATE_BASELINE_ROOT
export XIAMIMATE_LOG_DIR
export PG_HOST
export PG_PORT
export PG_DB
export PG_USER
export PG_PASSWORD
export PGPASSWORD
export PG_TUNNEL_ENABLED
export PG_TUNNEL_SSH_HOST
export PG_TUNNEL_LOCAL_HOST
export PG_TUNNEL_LOCAL_PORT
export PG_TUNNEL_REMOTE_HOST
export PG_TUNNEL_REMOTE_PORT
export PG_TUNNEL_PID_FILE
export PG_TUNNEL_LOG_FILE
export XIAMIMATE_COLLECTOR_FORCE_LOCAL_PG
export XIAMIMATE_COLLECTOR_LOCAL_PG_HOST
export XIAMIMATE_COLLECTOR_LOCAL_PG_PORT
export XIAMIMATE_COLLECTOR_LOCAL_PG_DB
export XIAMIMATE_COLLECTOR_LOCAL_PG_USER
export XIAMIMATE_COLLECTOR_LOCAL_PG_PASSWORD
