#!/usr/bin/env bash

set -euo pipefail

export PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
    cat <<'EOF'
Usage:
  bash scripts/manage_collector_jobs.sh help
  bash scripts/manage_collector_jobs.sh list
  bash scripts/manage_collector_jobs.sh status
  bash scripts/manage_collector_jobs.sh preview
  bash scripts/manage_collector_jobs.sh <job> <action>

Jobs:
  auto            原始采集: Keepa / Trends -> DuckDB + raw JSON
  pg-sync         主同步: DuckDB 规范化/聚合表 -> PostgreSQL 目标库
  theme-sync      主题特征同步: DuckDB serving 特征子集 -> PostgreSQL 目标库
  pg-tunnel       仅 SSH 隧道，不做任何数据同步
  week1           Week1 训练集/特征基建导出 -> Parquet/Manifest/Report
  validate        只读环境校验

Examples:
  bash scripts/manage_collector_jobs.sh auto status
  bash scripts/manage_collector_jobs.sh pg-sync preview
  bash scripts/manage_collector_jobs.sh theme-sync logs
  bash scripts/manage_collector_jobs.sh week1 preview

Key point:
  pg-sync 和 theme-sync 都是“同步到当前配置的 PostgreSQL 目标库”。
  是否直连本地 PostgreSQL、直连 RDS、还是通过 SSH 隧道到 RDS，不由脚本名决定，
  而由 data_collector/.env 里的 PG_* 和 PG_TUNNEL_* 配置决定。
EOF
}

describe_jobs() {
    cat <<'EOF'
[auto]
  脚本: scripts/manage_auto_collect.sh
  职责: 拉取原始采集数据并写入 DuckDB / raw JSON

[pg-sync]
  脚本: scripts/manage_pg_sync.sh
  职责: 把 DuckDB 中的规范化表、聚合表同步到 PostgreSQL
  目标: 本地 PG / RDS / SSH 隧道后的 RDS，取决于环境变量

[theme-sync]
    脚本: scripts/manage_theme_sync.sh
    职责: 把 DuckDB 中给在线 serving 用的主题特征子集同步到 PostgreSQL
    典型表: base / trends / cross

[pg-tunnel]
  脚本: scripts/manage_pg_ssh_tunnel.sh
  职责: 只管打通到 PostgreSQL 的 SSH 隧道，不做任何同步

[week1]
  脚本: scripts/manage_week1_foundation.sh
  职责: 从 DuckDB 导出 week1 训练特征与训练集 parquet

[validate]
  脚本: scripts/dry_run_validate_collector.sh
  职责: 只读校验当前 collector 环境配置
EOF
}

script_for_job() {
    case "$1" in
        auto|auto-collect)
            echo "$ROOT_DIR/scripts/manage_auto_collect.sh"
            ;;
        pg-sync|pg)
            echo "$ROOT_DIR/scripts/manage_pg_sync.sh"
            ;;
        theme-sync|theme)
            echo "$ROOT_DIR/scripts/manage_theme_sync.sh"
            ;;
        pg-tunnel|tunnel)
            echo "$ROOT_DIR/scripts/manage_pg_ssh_tunnel.sh"
            ;;
        week1|week1-foundation)
            echo "$ROOT_DIR/scripts/manage_week1_foundation.sh"
            ;;
        validate)
            echo "$ROOT_DIR/scripts/dry_run_validate_collector.sh"
            ;;
        *)
            return 1
            ;;
    esac
}

run_job() {
    local job="$1"
    shift
    local script
    script="$(script_for_job "$job")" || {
        echo "unknown job: $job" >&2
        usage >&2
        exit 1
    }

    if [[ "$job" == "validate" ]]; then
        exec /bin/bash "$script"
    fi

    if [[ $# -eq 0 ]]; then
        echo "missing action for job: $job" >&2
        usage >&2
        exit 1
    fi

    exec /bin/bash "$script" "$@"
}

show_all_status() {
    for item in auto pg-sync theme-sync pg-tunnel week1; do
        echo "=== $item ==="
        case "$item" in
            auto)
                /bin/bash "$ROOT_DIR/scripts/manage_auto_collect.sh" status || true
                ;;
            pg-sync)
                /bin/bash "$ROOT_DIR/scripts/manage_pg_sync.sh" status || true
                ;;
            theme-sync)
                /bin/bash "$ROOT_DIR/scripts/manage_theme_sync.sh" status || true
                ;;
            pg-tunnel)
                /bin/bash "$ROOT_DIR/scripts/manage_pg_ssh_tunnel.sh" status || true
                ;;
            week1)
                /bin/bash "$ROOT_DIR/scripts/manage_week1_foundation.sh" status || true
                ;;
        esac
        echo
    done
}

show_all_preview() {
    for item in auto pg-sync theme-sync pg-tunnel week1; do
        echo "=== $item ==="
        case "$item" in
            auto)
                /bin/bash "$ROOT_DIR/scripts/manage_auto_collect.sh" preview
                ;;
            pg-sync)
                /bin/bash "$ROOT_DIR/scripts/manage_pg_sync.sh" preview
                ;;
            theme-sync)
                /bin/bash "$ROOT_DIR/scripts/manage_theme_sync.sh" preview
                ;;
            pg-tunnel)
                /bin/bash "$ROOT_DIR/scripts/manage_pg_ssh_tunnel.sh" preview
                ;;
            week1)
                /bin/bash "$ROOT_DIR/scripts/manage_week1_foundation.sh" preview
                ;;
        esac
        echo
    done
}

case "${1:-help}" in
    help)
        usage
        echo
        describe_jobs
        ;;
    list)
        describe_jobs
        ;;
    status)
        show_all_status
        ;;
    preview)
        show_all_preview
        ;;
    *)
        job="$1"
        shift
        run_job "$job" "$@"
        ;;
esac