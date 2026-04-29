#!/usr/bin/env python3
"""DuckDB → PostgreSQL 同步脚本.

将 DuckDB curated 表全量/增量同步到 PostgreSQL sync schema,
供 Metabase (数据探索) 和 Grafana (采集监控) 使用.

用法:
    # 全量同步 (首次或重建)
    python sync_duckdb_to_pg.py --full

    # 增量同步 (仅新数据, 默认)
    python sync_duckdb_to_pg.py

    # 定时同步 (每 5 分钟)
    python sync_duckdb_to_pg.py --loop --interval 300

环境变量:
    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
    DUCKDB_PATH  (默认: data_platform/storage/warehouse/local_analytics.duckdb)
"""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import shutil
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import duckdb
except ImportError:
    sys.exit("duckdb is required: pip install duckdb")

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    sys.exit("psycopg2 is required: pip install psycopg2-binary")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("sync")


def _resolve_configured_path(env_name: str, default_path: Path) -> Path:
    configured = os.environ.get(env_name)
    if configured:
        return Path(configured).expanduser().resolve()
    return default_path.resolve()


# 项目根目录
PROJECT_ROOT = Path(__file__).resolve().parents[1]
INIT_SYNC_TABLES_SQL = _resolve_configured_path(
    "XIAMIMATE_INIT_SYNC_TABLES_SQL",
    PROJECT_ROOT / "data_platform" / "postgres" / "init_sync_tables.sql",
)

# 默认 DuckDB 路径
DEFAULT_DUCKDB_PATH = _resolve_configured_path(
    "XIAMIMATE_DUCKDB_PATH",
    PROJECT_ROOT / "data_platform" / "storage" / "warehouse" / "local_analytics.duckdb",
)

DEFAULT_LOCK_PATH = _resolve_configured_path(
    "XIAMIMATE_LOG_DIR",
    PROJECT_ROOT / "logs",
) / "sync_duckdb_to_pg.lock"
DEFAULT_SYNC_STATE_PATH = _resolve_configured_path(
    "XIAMIMATE_LOG_DIR",
    PROJECT_ROOT / "logs",
) / "sync_duckdb_to_pg.state.json"
PG_HISTORY_RETENTION_DAYS = 90
PG_SYNC_BATCH_SIZE = max(200, int(os.environ.get("PG_SYNC_BATCH_SIZE", "2000")))
PG_SYNC_FETCH_BATCH_SIZE = max(
    PG_SYNC_BATCH_SIZE,
    int(os.environ.get("PG_SYNC_FETCH_BATCH_SIZE", "10000")),
)
PG_AGG_REFRESH_INTERVAL_SECONDS = max(
    300, int(os.environ.get("PG_AGG_REFRESH_INTERVAL_SECONDS", "3600"))
)

HISTORY_DOMAIN_DAILY_SQL = f"""
SELECT
    h.date,
    h.domain,
    COUNT(*) AS rows_count,
    COUNT(DISTINCT h.asin) AS asin_count,
    AVG(COALESCE(h.buy_box_price, h.amazon_price, h.new_price)) AS avg_effective_price,
    MIN(COALESCE(h.buy_box_price, h.amazon_price, h.new_price)) AS min_effective_price,
    MAX(COALESCE(h.buy_box_price, h.amazon_price, h.new_price)) AS max_effective_price,
    AVG(h.bsr) AS avg_bsr,
    MIN(h.bsr) AS best_bsr,
    AVG(h.rating) AS avg_rating,
    AVG(h.review_count) AS avg_review_count,
    SUM(COALESCE(h.monthly_sold, 0)) AS sum_monthly_sold,
    AVG(h.monthly_sold) AS avg_monthly_sold,
    MAX(h.ingested_at) AS aggregated_at
FROM curated.keepa_product_history h
GROUP BY 1, 2
"""

HISTORY_ROOT_CATEGORY_DAILY_SQL = f"""
SELECT
    h.date,
    h.domain,
    COALESCE(r.root_category_id, r.category_id, 0) AS root_category_id,
    CASE
        WHEN COALESCE(r.root_category_id, r.category_id, 0) = 0 THEN 'Unknown'
        ELSE MAX(COALESCE(c.category_cn, c.category_en, r.category, 'Unknown'))
    END AS root_category_name,
    COUNT(*) AS rows_count,
    COUNT(DISTINCT h.asin) AS asin_count,
    AVG(COALESCE(h.buy_box_price, h.amazon_price, h.new_price)) AS avg_effective_price,
    MIN(COALESCE(h.buy_box_price, h.amazon_price, h.new_price)) AS min_effective_price,
    MAX(COALESCE(h.buy_box_price, h.amazon_price, h.new_price)) AS max_effective_price,
    AVG(h.bsr) AS avg_bsr,
    MIN(h.bsr) AS best_bsr,
    AVG(h.rating) AS avg_rating,
    AVG(h.review_count) AS avg_review_count,
    SUM(COALESCE(h.monthly_sold, 0)) AS sum_monthly_sold,
    AVG(h.monthly_sold) AS avg_monthly_sold,
    MAX(h.ingested_at) AS aggregated_at
FROM curated.keepa_product_history h
LEFT JOIN curated.keepa_asin_registry r
    ON h.asin = r.asin AND h.domain = r.domain
LEFT JOIN curated.keepa_category_registry c
    ON COALESCE(r.root_category_id, r.category_id) = c.category_id AND h.domain = c.domain
GROUP BY 1, 2, 3
"""

# DuckDB 表 → PG 表映射 + 主键列
SYNC_TABLES = {
    "curated.keepa_asin_registry": {
        "pg_table": "sync.keepa_asin_registry",
        "pk": ["asin", "domain"],
        "timestamp_col": "first_seen_at",  # 用于增量同步
        "timestamp_cols": ["first_seen_at", "last_fetched_at", "inactive_at"],
    },
    "curated.keepa_product_snapshot": {
        "pg_table": "sync.keepa_product_snapshot",
        "pk": ["asin", "domain"],
        "timestamp_col": "ingested_at",
        "timestamp_cols": ["data_capture_time", "ingested_at"],
    },
    "curated.keepa_product_history": {
        "pg_table": "sync.keepa_product_history",
        "pk": ["asin", "domain", "date"],
        "timestamp_col": "ingested_at",
        "where_clause": f"date >= CURRENT_DATE - INTERVAL '{PG_HISTORY_RETENTION_DAYS} days'",
        "retention_days": PG_HISTORY_RETENTION_DAYS,
        "retention_column": "date",
    },
    "agg.keepa_history_domain_daily": {
        "pg_table": "sync.keepa_history_domain_daily",
        "pk": ["date", "domain"],
        "timestamp_col": "aggregated_at",
        "duck_sql": HISTORY_DOMAIN_DAILY_SQL,
        "always_full_refresh": True,
        "min_sync_interval_seconds": PG_AGG_REFRESH_INTERVAL_SECONDS,
    },
    "agg.keepa_history_root_category_daily": {
        "pg_table": "sync.keepa_history_root_category_daily",
        "pk": ["date", "domain", "root_category_id"],
        "timestamp_col": "aggregated_at",
        "duck_sql": HISTORY_ROOT_CATEGORY_DAILY_SQL,
        "always_full_refresh": True,
        "min_sync_interval_seconds": PG_AGG_REFRESH_INTERVAL_SECONDS,
    },
    "curated.google_trends_daily": {
        "pg_table": "sync.google_trends_daily",
        "pk": ["keyword", "geo", "date"],
        "timestamp_col": "ingested_at",
    },
    "curated.collection_log": {
        "pg_table": "sync.collection_log",
        "pk": [],  # 无主键, 用 finished_at 增量
        "timestamp_col": "finished_at",
        "skip_columns": ["job_id"],  # PG 用 SERIAL 自增
    },
    "curated.asin_keyword_mapping": {
        "pg_table": "sync.asin_keyword_mapping",
        "pk": ["asin", "domain", "keyword"],
        "timestamp_col": "created_at",
    },
    "curated.asin_raw_file_mapping": {
        "pg_table": "sync.asin_raw_file_mapping",
        "pk": ["asin", "domain", "raw_file_path"],
        "timestamp_col": "created_at",
    },
    "curated.discovery_expansion_state": {
        "pg_table": "sync.discovery_expansion_state",
        "pk": ["expansion_type", "domain", "target_key"],
        "timestamp_col": "last_run_at",
        "timestamp_cols": ["first_run_at", "last_run_at"],
    },
    "curated.keepa_category_registry": {
        "pg_table": "sync.keepa_category_registry",
        "pk": ["category_id", "domain"],
        "timestamp_col": "created_at",
        "timestamp_cols": ["created_at", "bestseller_fetched_at", "children_fetched_at"],
    },
}


def get_pg_conn():
    """创建 PostgreSQL 连接."""
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ.get("PG_DB", "xiamimate"),
        user=os.environ.get("PG_USER", "xiamimate"),
        password=os.environ.get("PG_PASSWORD", "xiamimate"),
    )


def acquire_process_lock(lock_path: str | Path):
    """Acquire a non-blocking file lock so only one sync process can run."""
    path = Path(lock_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("w", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        sys.exit(f"同步进程已在运行, 请勿重复启动: {path}")

    handle.write(f"pid={os.getpid()}\n")
    handle.flush()
    return handle


def ensure_pg_sync_schema(pg_conn) -> None:
    """Ensure sync schema/tables exist and apply lightweight schema migrations."""
    if not INIT_SYNC_TABLES_SQL.exists():
        raise FileNotFoundError(f"init_sync_tables.sql not found: {INIT_SYNC_TABLES_SQL}")

    sql = INIT_SYNC_TABLES_SQL.read_text(encoding="utf-8")
    with pg_conn.cursor() as cur:
        cur.execute(sql)
    pg_conn.commit()


def get_duckdb_conn(db_path: str | Path) -> duckdb.DuckDBPyConnection:
    """拷贝 DuckDB 文件后以只读方式打开 (避免与 auto_collect 冲突)."""
    src = Path(db_path)
    tmp_dir = Path(tempfile.gettempdir()) / "xiamimate_sync"
    tmp_dir.mkdir(exist_ok=True)
    tmp_path = tmp_dir / src.name
    # 拷贝主文件 + WAL (如果存在), 保证包含最新未 checkpoint 数据
    shutil.copy2(src, tmp_path)
    wal = src.with_suffix(".duckdb.wal")
    if wal.exists():
        shutil.copy2(wal, tmp_dir / wal.name)
    try:
        return duckdb.connect(str(tmp_path), read_only=True)
    except Exception as exc:
        logger.warning(
            "打开 DuckDB 临时快照失败，将回退到源库只读连接: %s", exc
        )
        return duckdb.connect(str(src), read_only=True)


def _get_pg_max_timestamp(pg_conn, pg_table: str, ts_col: str):
    """获取 PG 表中最大时间戳, 用于增量同步.

    返回 datetime 对象 (带时区或无时区, 取决于 PG 列类型).
    """
    with pg_conn.cursor() as cur:
        cur.execute(f"SELECT MAX({ts_col}) FROM {pg_table}")
        row = cur.fetchone()
        return row[0] if row and row[0] else None


def _get_pg_max_timestamp_multi(pg_conn, pg_table: str, ts_cols: list[str]):
    """获取 PG 表中每个时间列各自的最大值, 返回 {col: max_ts} 字典."""
    result = {}
    with pg_conn.cursor() as cur:
        for col in ts_cols:
            cur.execute(f"SELECT MAX({col}) FROM {pg_table}")
            row = cur.fetchone()
            if row and row[0]:
                result[col] = row[0]
    return result if result else None


def _build_greatest_expr(ts_cols: list[str]) -> str:
    """构建 DuckDB 的 GREATEST() 表达式, 用于增量 WHERE."""
    if len(ts_cols) == 1:
        return ts_cols[0]
    parts = [f"COALESCE({c}, '1970-01-01'::TIMESTAMP)" for c in ts_cols]
    return f"GREATEST({', '.join(parts)})"


def _pg_ts_to_duck_ts(pg_ts) -> str:
    """将 PG 的 UTC 时间戳转为 DuckDB 的 CST naive 字符串, 用于 WHERE 比较.

    PG 存 TIMESTAMPTZ (UTC), DuckDB 存 naive CST → 需要 +8h.
    """
    if pg_ts is None:
        return None
    if hasattr(pg_ts, "tzinfo") and pg_ts.tzinfo is not None:
        # 转为 CST
        cst = pg_ts + timedelta(hours=8)
        return cst.strftime("%Y-%m-%d %H:%M:%S.%f")
    # 无时区则原样返回
    return str(pg_ts)


def _build_duck_select(duck_source: str, config: dict) -> str:
    if config.get("duck_sql"):
        return f"SELECT * FROM ({config['duck_sql']}) AS src"
    return f"SELECT * FROM {duck_source}"


def _append_duck_where(base_query: str, where_clause: str) -> str:
    return f"SELECT * FROM ({base_query}) AS scoped_src WHERE {where_clause}"


def _apply_pg_retention(pg_conn, pg_table: str, retention_column: str, retention_days: int) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {pg_table} WHERE {retention_column} < CURRENT_DATE - INTERVAL %s",
            [f"{int(retention_days)} days"],
        )


def _load_sync_state(state_path: str | Path) -> dict:
    path = Path(state_path)
    if not path.exists():
        return {}

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("读取同步状态文件失败，将忽略旧状态: %s", exc)
        return {}


def _save_sync_state(state_path: str | Path, state: dict) -> None:
    path = Path(state_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def _should_skip_table(config: dict, full: bool, sync_state: dict) -> bool:
    if full:
        return False

    min_sync_interval_seconds = int(config.get("min_sync_interval_seconds", 0) or 0)
    if min_sync_interval_seconds <= 0:
        return False

    last_synced_at = sync_state.get(config["pg_table"])
    if not last_synced_at:
        return False

    try:
        last_synced_dt = datetime.fromisoformat(last_synced_at)
    except ValueError:
        return False

    now = datetime.now(timezone.utc)
    return (now - last_synced_dt).total_seconds() < min_sync_interval_seconds


def _deduplicate_rows_by_pk(rows: list[tuple], columns: list[str], pk: list[str]) -> tuple[list[tuple], int]:
    if not pk or not rows:
        return rows, 0

    pk_indexes = [columns.index(column) for column in pk]
    deduplicated: dict[tuple, tuple] = {}
    for row in rows:
        key = tuple(row[index] for index in pk_indexes)
        deduplicated[key] = row

    duplicate_count = len(rows) - len(deduplicated)
    if duplicate_count <= 0:
        return rows, 0

    return list(deduplicated.values()), duplicate_count


def _detect_datetime_indexes(rows: list[tuple]) -> list[int]:
    if not rows:
        return []

    datetime_indexes = []
    for index in range(len(rows[0])):
        for row in rows:
            value = row[index]
            if value is None:
                continue
            if isinstance(value, datetime):
                datetime_indexes.append(index)
            break
    return datetime_indexes


def _normalize_batch_rows(
    rows: list[tuple],
    ts_indexes: list[int],
    keep_indexes: list[int] | None,
) -> list[tuple]:
    if not rows:
        return []

    if not ts_indexes and keep_indexes is None:
        return rows

    cst_offset = timedelta(hours=8)
    normalized_rows = []
    for raw_row in rows:
        row = list(raw_row)
        for idx in ts_indexes:
            if row[idx] is not None:
                row[idx] = row[idx] - cst_offset
        if keep_indexes is not None:
            row = [row[idx] for idx in keep_indexes]
        normalized_rows.append(tuple(row))
    return normalized_rows


def sync_table(
    duck: duckdb.DuckDBPyConnection,
    pg_conn,
    duck_table: str,
    config: dict,
    full: bool = False,
) -> int:
    """同步单张表, 返回写入行数."""
    started_at = time.time()
    pg_table = config["pg_table"]
    pk = config["pk"]
    ts_col = config["timestamp_col"]
    effective_full = full or config.get("always_full_refresh", False)
    base_query = _build_duck_select(duck_source=duck_table, config=config)
    filtered_query = base_query
    if config.get("where_clause"):
        filtered_query = _append_duck_where(base_query, config["where_clause"])

    # 构建 SELECT
    if effective_full:
        query = filtered_query
    else:
        # 多时间列: 每列独立比较各自的 MAX, 用 OR 连接, 避免遗漏交叉更新
        ts_cols_list = config.get("timestamp_cols", [ts_col])
        max_ts_map = _get_pg_max_timestamp_multi(pg_conn, pg_table, ts_cols_list)
        if max_ts_map:
            # PG 存 UTC, DuckDB 存 CST naive → 转换后再比较
            conditions = []
            for col, pg_ts in max_ts_map.items():
                duck_ts = _pg_ts_to_duck_ts(pg_ts)
                conditions.append(f"{col} > '{duck_ts}'")
            where_clause = " OR ".join(conditions)
            query = _append_duck_where(filtered_query, where_clause)
        else:
            query = filtered_query

    try:
        result = duck.execute(query)
        source_columns = [desc[0] for desc in result.description]
        first_batch = result.fetchmany(PG_SYNC_FETCH_BATCH_SIZE)
    except Exception as e:
        logger.warning(f"读取 {duck_table} 失败: {e}")
        return 0

    if not first_batch:
        logger.info(f"  {duck_table} → 无新数据")
        return 0

    # 跳过 PG 自增列
    skip = set(config.get("skip_columns", []))
    if skip:
        keep_idx = [i for i, c in enumerate(source_columns) if c not in skip]
        columns = [source_columns[i] for i in keep_idx]
    else:
        keep_idx = None
        columns = source_columns

    # 使用 UPSERT (INSERT ON CONFLICT) 或普通 INSERT
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))

    if pk:
        pk_list = ", ".join(pk)
        update_cols = [c for c in columns if c not in pk]
        if update_cols:
            update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
            sql = (
                f"INSERT INTO {pg_table} ({col_list}) VALUES ({placeholders}) "
                f"ON CONFLICT ({pk_list}) DO UPDATE SET {update_set}"
            )
        else:
            sql = (
                f"INSERT INTO {pg_table} ({col_list}) VALUES ({placeholders}) "
                f"ON CONFLICT ({pk_list}) DO NOTHING"
            )
    else:
        sql = f"INSERT INTO {pg_table} ({col_list}) VALUES ({placeholders})"

    total_rows = 0
    fetch_batch_count = 0
    dropped_duplicate_rows = 0

    with pg_conn.cursor() as cur:
        retention_days = config.get("retention_days")
        retention_column = config.get("retention_column")
        if retention_days and retention_column:
            _apply_pg_retention(pg_conn, pg_table, retention_column, int(retention_days))
        # 全量同步: 先清空
        if effective_full and pk:
            cur.execute(f"TRUNCATE TABLE {pg_table}")
        elif effective_full and not pk:
            cur.execute(f"DELETE FROM {pg_table}")
        elif not effective_full and not pk and ts_col:
            # 无主键表增量: 先删除 >= max_ts 的旧行, 防止重复
            max_ts = _get_pg_max_timestamp(pg_conn, pg_table, ts_col)
            if max_ts:
                cur.execute(
                    f"DELETE FROM {pg_table} WHERE {ts_col} >= %s", [max_ts]
                )

        raw_rows = first_batch
        while raw_rows:
            fetch_batch_count += 1
            ts_indexes = _detect_datetime_indexes(raw_rows)
            rows = _normalize_batch_rows(raw_rows, ts_indexes, keep_idx)
            rows, duplicate_count = _deduplicate_rows_by_pk(rows, columns, pk)
            dropped_duplicate_rows += duplicate_count
            if duplicate_count > 0:
                logger.warning(
                    "  %s → %s: dropped %s duplicate rows for PK %s before batch upsert",
                    duck_table,
                    pg_table,
                    duplicate_count,
                    ", ".join(pk),
                )

            if rows:
                psycopg2.extras.execute_values(
                    cur,
                    sql.replace(f"VALUES ({placeholders})", "VALUES %s"),
                    rows,
                    page_size=PG_SYNC_BATCH_SIZE,
                )
                total_rows += len(rows)

            raw_rows = result.fetchmany(PG_SYNC_FETCH_BATCH_SIZE)

    pg_conn.commit()
    elapsed = round(time.time() - started_at, 1)
    duplicate_suffix = ""
    if dropped_duplicate_rows > 0:
        duplicate_suffix = f", dropped {dropped_duplicate_rows} duplicate rows"
    logger.info(
        f"  {duck_table} → {pg_table}: {total_rows} rows in {elapsed}s across {fetch_batch_count} fetch batches{duplicate_suffix}"
    )
    return total_rows


def _reconcile_candidate_expansion_jobs_completion(pg_conn) -> int:
    """Mark expansion jobs completed once their discovered ASINs are visible in PG."""
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            WITH progress AS (
                SELECT
                    j.job_id,
                    CARDINALITY(COALESCE(j.result_candidate_asins, ARRAY[]::TEXT[])) AS expected_asin_count,
                    COUNT(DISTINCT r.asin) AS synced_asin_count
                FROM sync.keepa_candidate_expansion_jobs j
                LEFT JOIN sync.keepa_asin_registry r
                  ON r.domain = j.domain
                 AND r.asin = ANY(COALESCE(j.result_candidate_asins, ARRAY[]::TEXT[]))
                WHERE j.status = 'syncing'
                GROUP BY j.job_id, j.result_candidate_asins
            ),
            ready_jobs AS (
                SELECT job_id, expected_asin_count, synced_asin_count
                FROM progress
                WHERE expected_asin_count = 0
                   OR synced_asin_count >= expected_asin_count
            )
            UPDATE sync.keepa_candidate_expansion_jobs j
            SET status = 'completed',
                status_reason = CASE
                    WHEN ready_jobs.expected_asin_count = 0 THEN 'No ASINs returned; PostgreSQL sync not required'
                    ELSE 'Expansion ASINs synced to PostgreSQL'
                END,
                updated_at = NOW(),
                finished_at = NOW(),
                meta_json = COALESCE(j.meta_json, '{}'::JSONB)
                    || jsonb_build_object(
                        'pg_expected_asin_count', ready_jobs.expected_asin_count,
                        'pg_synced_asin_count', ready_jobs.synced_asin_count,
                        'sync_reconciled_at', NOW()
                    )
            FROM ready_jobs
            WHERE j.job_id = ready_jobs.job_id
            RETURNING j.job_id
            """
        )
        completed = cur.fetchall()
    pg_conn.commit()

    completed_count = len(completed)
    if completed_count:
        logger.info("  expansion jobs: marked %s syncing jobs as completed", completed_count)
    return completed_count


def run_sync(duckdb_path: str | Path, full: bool = False, state_path: str | Path = DEFAULT_SYNC_STATE_PATH) -> dict:
    """执行一轮同步."""
    logger.info(f"{'全量' if full else '增量'}同步开始 — DuckDB: {duckdb_path}")
    start = time.time()
    results = {}
    sync_state = _load_sync_state(state_path)

    duck = get_duckdb_conn(duckdb_path)
    pg_conn = get_pg_conn()

    try:
        ensure_pg_sync_schema(pg_conn)
        for duck_table, config in SYNC_TABLES.items():
            try:
                if _should_skip_table(config, full=full, sync_state=sync_state):
                    interval_seconds = int(config.get("min_sync_interval_seconds", 0) or 0)
                    logger.info(
                        "  %s → %s: skipped (refresh interval %ss not reached)",
                        duck_table,
                        config["pg_table"],
                        interval_seconds,
                    )
                    results[duck_table] = 0
                    continue

                count = sync_table(duck, pg_conn, duck_table, config, full=full)
                results[duck_table] = count
                sync_state[config["pg_table"]] = datetime.now(timezone.utc).isoformat()
            except Exception as e:
                logger.error(f"  同步 {duck_table} 失败: {e}")
                pg_conn.rollback()
                results[duck_table] = -1
        try:
            results["sync.keepa_candidate_expansion_jobs:reconciled"] = _reconcile_candidate_expansion_jobs_completion(pg_conn)
        except Exception as e:
            logger.warning("  expansion jobs 状态协调失败: %s", e)
            pg_conn.rollback()
            results["sync.keepa_candidate_expansion_jobs:reconciled"] = -1
    finally:
        duck.close()
        pg_conn.close()

    _save_sync_state(state_path, sync_state)

    elapsed = round(time.time() - start, 1)
    total = sum(v for v in results.values() if v > 0)
    logger.info(f"同步完成 — {total} rows in {elapsed}s")
    return results


def main():
    parser = argparse.ArgumentParser(description="DuckDB → PostgreSQL 同步")
    parser.add_argument("--full", action="store_true", help="全量同步 (TRUNCATE + INSERT)")
    parser.add_argument("--loop", action="store_true", help="循环执行")
    parser.add_argument("--interval", type=int, default=300, help="循环间隔秒数 (默认 300)")
    parser.add_argument("--duckdb-path", type=str, default=None, help="DuckDB 文件路径")
    parser.add_argument("--lock-file", type=str, default=str(DEFAULT_LOCK_PATH), help="单实例锁文件路径")
    args = parser.parse_args()

    db_path = args.duckdb_path or DEFAULT_DUCKDB_PATH
    lock_handle = acquire_process_lock(args.lock_file)

    if not Path(db_path).exists():
        sys.exit(f"DuckDB 文件不存在: {db_path}")

    if args.loop:
        logger.info(f"定时同步模式 — 间隔 {args.interval}s")
        while True:
            try:
                run_sync(db_path, full=args.full)
                # 第一轮全量后改为增量
                args.full = False
            except Exception as e:
                logger.error(f"同步异常: {e}")
            time.sleep(args.interval)
    else:
        run_sync(db_path, full=args.full)

    lock_handle.close()


if __name__ == "__main__":
    main()
