#!/usr/bin/env python3
"""Sync DuckDB-derived theme serving features into PostgreSQL.

This script rebuilds the serving feature subsets directly from DuckDB curated tables
and syncs them into PostgreSQL, so the online API does not depend on training-set
Parquet outputs.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import fcntl
import logging
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    import duckdb
except ImportError:
    sys.exit("duckdb is required: pip install duckdb")

try:
    import psycopg2
except ImportError:
    sys.exit("psycopg2 is required: pip install psycopg2-binary")

from data_collector.sales_forecast.bsr_sales_converter import (
    CATEGORY_COEFFICIENTS,
    DEFAULT_COEFFICIENTS,
    DOMAIN_MULTIPLIER,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("theme-feature-sync")


def _resolve_configured_path(env_name: str, default_path: Path) -> Path:
    configured = os.environ.get(env_name)
    if configured:
        return Path(configured).expanduser().resolve()
    return default_path.resolve()


INIT_SYNC_TABLES_SQL = _resolve_configured_path(
    "XIAMIMATE_INIT_SYNC_TABLES_SQL",
    PROJECT_ROOT / "data_platform" / "postgres" / "init_sync_tables.sql",
)
DEFAULT_DUCKDB_PATH = _resolve_configured_path(
    "XIAMIMATE_DUCKDB_PATH",
    PROJECT_ROOT / "data_platform" / "storage" / "warehouse" / "local_analytics.duckdb",
)
DEFAULT_LOCK_PATH = _resolve_configured_path(
    "XIAMIMATE_LOG_DIR",
    PROJECT_ROOT / "logs",
) / "sync_theme_features_to_pg.lock"

DEFAULT_RETENTION_DAYS = max(30, int(os.environ.get("THEME_FEATURE_RETENTION_DAYS", "180")))
DEFAULT_OVERLAP_DAYS = max(7, int(os.environ.get("THEME_FEATURE_REFRESH_OVERLAP_DAYS", "35")))
DEFAULT_DUCKDB_THREADS = max(1, int(os.environ.get("THEME_FEATURE_DUCKDB_THREADS", "4")))
THEME_FEATURE_DUCKDB_READ_MODE = os.environ.get("THEME_FEATURE_DUCKDB_READ_MODE", "direct").strip().lower()
THEME_FEATURE_DUCKDB_COPY_DIR = os.environ.get("THEME_FEATURE_DUCKDB_COPY_DIR", "").strip()
THEME_FEATURE_TARGET_ASINS = os.environ.get("THEME_FEATURE_TARGET_ASINS", "").strip()
THEME_FEATURE_TARGET_DOMAINS = os.environ.get("THEME_FEATURE_TARGET_DOMAINS", "").strip()
SERVING_LOOKBACK_DAYS = 7
_SYNC_COPY_DIRS: list[Path] = []

FEATURE_TABLES = {
    "base": {
        "pg_table": "serving.theme_base_daily",
        "source_table": "theme_sync_base_daily",
        "columns": [
            "domain",
            "asin",
            "date",
            "product_title",
            "brand",
            "category",
            "effective_price",
            "rating",
            "review_count",
            "new_offer_count",
            "used_offer_count",
            "bsr",
            "estimated_daily_sales",
        ],
    },
    "trends": {
        "pg_table": "serving.theme_trends_daily",
        "source_table": "theme_sync_trends_daily",
        "columns": [
            "domain",
            "asin",
            "date",
            "trend_index_mean",
            "trend_index_wow",
            "trend_index_dod",
            "trend_index_roll_std_7",
            "trend_index_roll_max_7",
            "trend_keyword_coverage_ratio",
        ],
    },
    "cross": {
        "pg_table": "serving.theme_cross_daily",
        "source_table": "theme_sync_cross_daily",
        "columns": [
            "domain",
            "asin",
            "date",
            "product_title",
            "effective_price",
            "bsr",
            "rating",
            "review_count",
            "new_offer_count",
            "used_offer_count",
            "estimated_daily_sales",
            "trend_index_mean",
            "price_discount_pct",
        ],
    },
}


def _domain_multiplier_case() -> str:
    when_clauses = "\n".join(
        f"WHEN domain = {domain} THEN {multiplier}"
        for domain, multiplier in sorted(DOMAIN_MULTIPLIER.items())
    )
    return f"CASE\n{when_clauses}\nELSE 1.0 END"


def _category_coeff_case(field_name: str) -> str:
    when_clauses = "\n".join(
        f"WHEN COALESCE(root_category_id, category_id, 0) = {category_id} THEN {getattr(coeff, field_name)}"
        for category_id, coeff in sorted(CATEGORY_COEFFICIENTS.items())
    )
    default_value = getattr(DEFAULT_COEFFICIENTS, field_name)
    return f"CASE\n{when_clauses}\nELSE {default_value} END"


THEME_BASE_SOURCE_SQL = """
WITH history_enriched AS (
    SELECT
        h.asin,
        h.domain,
        h.date,
        h.amazon_price,
        h.new_price,
        h.buy_box_price,
        h.list_price,
        h.bsr,
        h.rating,
        h.review_count,
        h.monthly_sold,
        h.new_offer_count,
        h.used_offer_count,
        r.product_title,
        r.brand,
        r.category,
        r.category_id,
        r.root_category_id,
        LAST_VALUE(h.amazon_price IGNORE NULLS) OVER history_window AS amazon_price_ffill,
        LAST_VALUE(h.new_price IGNORE NULLS) OVER history_window AS new_price_ffill,
        LAST_VALUE(h.buy_box_price IGNORE NULLS) OVER history_window AS buy_box_price_ffill,
        LAST_VALUE(h.list_price IGNORE NULLS) OVER history_window AS list_price_ffill,
        LAST_VALUE(h.bsr IGNORE NULLS) OVER history_window AS bsr_ffill,
        LAST_VALUE(h.rating IGNORE NULLS) OVER history_window AS rating_ffill,
        LAST_VALUE(h.review_count IGNORE NULLS) OVER history_window AS review_count_ffill,
        LAST_VALUE(h.new_offer_count IGNORE NULLS) OVER history_window AS new_offer_count_ffill,
        LAST_VALUE(h.used_offer_count IGNORE NULLS) OVER history_window AS used_offer_count_ffill
    FROM curated.keepa_product_history h
    LEFT JOIN curated.keepa_asin_registry r
        ON h.asin = r.asin AND h.domain = r.domain
        WHERE h.date >= DATE '{history_start_date}'
            {target_history_filter}
    WINDOW history_window AS (
        PARTITION BY h.asin, h.domain
        ORDER BY h.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
),
base_features AS (
    SELECT
        domain,
        asin,
        date,
        product_title,
        brand,
        category,
        COALESCE(buy_box_price_ffill, amazon_price_ffill, new_price_ffill) AS effective_price,
        rating_ffill AS rating,
        review_count_ffill AS review_count,
        new_offer_count_ffill AS new_offer_count,
        used_offer_count_ffill AS used_offer_count,
        bsr_ffill AS bsr,
        CASE
            WHEN list_price_ffill > 0 AND COALESCE(buy_box_price_ffill, amazon_price_ffill, new_price_ffill) IS NOT NULL THEN
                ((list_price_ffill - COALESCE(buy_box_price_ffill, amazon_price_ffill, new_price_ffill)) / list_price_ffill) * 100
            ELSE NULL
        END AS price_discount_pct,
        CASE
            WHEN monthly_sold IS NOT NULL AND monthly_sold > 0 THEN monthly_sold / 30.0
            WHEN bsr_ffill IS NOT NULL AND bsr_ffill > 0 THEN ({domain_multiplier_case}) * ({coeff_a_case}) * POWER(CAST(bsr_ffill AS DOUBLE), {coeff_b_case})
            ELSE NULL
        END AS estimated_daily_sales
    FROM history_enriched
)
SELECT
    domain,
    asin,
    date,
    product_title,
    brand,
    category,
    effective_price,
    rating,
    review_count,
    new_offer_count,
    used_offer_count,
    bsr,
    estimated_daily_sales,
    price_discount_pct
FROM base_features
WHERE date >= DATE '{source_start_date}'
"""


THEME_TRENDS_SOURCE_SQL = """
WITH keyword_totals AS (
    SELECT asin, domain, COUNT(DISTINCT keyword) AS total_keywords
    FROM curated.asin_keyword_mapping m
    {target_keyword_where}
    GROUP BY 1, 2
),
trend_daily AS (
    SELECT
        m.asin,
        m.domain,
        t.date,
        COUNT(DISTINCT m.keyword) AS trend_keyword_hits,
        AVG(t.trend_index) AS trend_index_mean
    FROM curated.asin_keyword_mapping m
    JOIN curated.google_trends_daily t
        ON m.keyword = t.keyword
       AND m.geo = t.geo
    WHERE t.date >= DATE '{source_start_date}'
            {target_keyword_filter}
    GROUP BY 1, 2, 3
),
aligned AS (
    SELECT
        b.domain,
        b.asin,
        b.date,
        k.total_keywords,
        td.trend_keyword_hits,
        td.trend_index_mean
    FROM theme_sync_base_daily b
    LEFT JOIN keyword_totals k
        ON b.asin = k.asin AND b.domain = k.domain
    LEFT JOIN trend_daily td
        ON b.asin = td.asin
       AND b.domain = td.domain
       AND b.date = td.date
)
SELECT
    domain,
    asin,
    date,
    trend_index_mean,
    trend_index_mean - LAG(trend_index_mean) OVER trend_window AS trend_index_dod,
    trend_index_mean - LAG(trend_index_mean, 7) OVER trend_window AS trend_index_wow,
    STDDEV_SAMP(trend_index_mean) OVER trend_roll_7 AS trend_index_roll_std_7,
    MAX(trend_index_mean) OVER trend_roll_7 AS trend_index_roll_max_7,
    CASE
        WHEN total_keywords > 0 THEN CAST(COALESCE(trend_keyword_hits, 0) AS DOUBLE) / total_keywords
        ELSE NULL
    END AS trend_keyword_coverage_ratio
FROM aligned
WINDOW
    trend_window AS (PARTITION BY asin, domain ORDER BY date),
    trend_roll_7 AS (PARTITION BY asin, domain ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
"""


THEME_CROSS_SOURCE_SQL = """
SELECT
    b.domain,
    b.asin,
    b.date,
    b.product_title,
    b.effective_price,
    b.bsr,
    b.rating,
    b.review_count,
    b.new_offer_count,
    b.used_offer_count,
    b.estimated_daily_sales,
    t.trend_index_mean,
    b.price_discount_pct
FROM theme_sync_base_daily b
LEFT JOIN theme_sync_trends_daily t
    ON b.asin = t.asin
   AND b.domain = t.domain
   AND b.date = t.date
"""


def get_pg_conn():
    conn = psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ.get("PG_DB", "xiamimate"),
        user=os.environ.get("PG_USER", "xiamimate"),
        password=os.environ.get("PG_PASSWORD", "xiamimate"),
    )
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = 0")
    conn.commit()
    return conn


def acquire_process_lock(lock_path: str | Path):
    path = Path(lock_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("w", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        sys.exit(f"主题特征同步进程已在运行, 请勿重复启动: {path}")

    handle.write(f"pid={os.getpid()}\n")
    handle.flush()
    return handle


def ensure_pg_schema(pg_conn) -> None:
    if not INIT_SYNC_TABLES_SQL.exists():
        raise FileNotFoundError(f"init_sync_tables.sql not found: {INIT_SYNC_TABLES_SQL}")

    with pg_conn.cursor() as cur:
        cur.execute(INIT_SYNC_TABLES_SQL.read_text(encoding="utf-8"))
    pg_conn.commit()


def _copy_duckdb_for_sync(src: Path) -> tuple[Path, Path]:
    base_dir = Path(THEME_FEATURE_DUCKDB_COPY_DIR).expanduser() if THEME_FEATURE_DUCKDB_COPY_DIR else Path(tempfile.gettempdir()) / "xiamimate_theme_feature_sync"
    base_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir = Path(tempfile.mkdtemp(prefix="run_", dir=base_dir))
    tmp_path = tmp_dir / src.name

    shutil.copy2(src, tmp_path)
    wal = Path(f"{src}.wal")
    if wal.exists():
        shutil.copy2(wal, tmp_dir / wal.name)

    return tmp_path, tmp_dir


def get_duckdb_conn(db_path: str | Path) -> duckdb.DuckDBPyConnection:
    """Open DuckDB for theme sync. Default mode reads a temporary copy to avoid live writer locks."""
    src = Path(db_path).resolve()
    retries = max(1, int(os.environ.get("DUCKDB_OPEN_RETRIES", "6")))
    base_delay = max(0.2, float(os.environ.get("DUCKDB_OPEN_RETRY_DELAY_SECONDS", "1.0")))
    use_copy = THEME_FEATURE_DUCKDB_READ_MODE not in {"live", "direct"}

    conn = None
    for attempt in range(1, retries + 1):
        copy_dir: Path | None = None
        try:
            db_to_open = src
            if use_copy:
                db_to_open, copy_dir = _copy_duckdb_for_sync(src)
                logger.info("DuckDB theme sync copy prepared: %s", db_to_open)

            conn = duckdb.connect(str(db_to_open), read_only=True)
            if copy_dir is not None:
                _SYNC_COPY_DIRS.append(copy_dir)
            break
        except Exception as exc:
            if copy_dir is not None:
                shutil.rmtree(copy_dir, ignore_errors=True)
            if attempt >= retries:
                raise
            wait_seconds = min(base_delay * (2 ** (attempt - 1)), 12.0)
            logger.warning(
                "DuckDB 连接失败(第 %d/%d 次)，%.1fs 后重试: %s",
                attempt,
                retries,
                wait_seconds,
                exc,
            )
            time.sleep(wait_seconds)

    if conn is None:
        conn = duckdb.connect(str(src), read_only=True)

    tmp_dir = Path(tempfile.gettempdir()) / "xiamimate_theme_feature_sync"
    tmp_dir.mkdir(exist_ok=True)
    conn.execute("SET preserve_insertion_order = false")
    conn.execute(f"SET threads TO {DEFAULT_DUCKDB_THREADS}")
    conn.execute(f"SET temp_directory = '{tmp_dir.as_posix()}'")
    return conn


def _current_utc_date():
    return datetime.now(timezone.utc).date()


def _retention_start_date(retention_days: int):
    return _current_utc_date() - timedelta(days=max(retention_days - 1, 0))


def _get_pg_max_date(pg_conn, pg_table: str):
    with pg_conn.cursor() as cur:
        cur.execute(f"SELECT MAX(date) FROM {pg_table}")
        row = cur.fetchone()
        return row[0] if row and row[0] is not None else None


def _compute_refresh_start(*, pg_conn, pg_table: str, retention_days: int, overlap_days: int, full: bool):
    retention_start = _retention_start_date(retention_days)
    if full:
        return retention_start

    max_date = _get_pg_max_date(pg_conn, pg_table)
    if max_date is None:
        return retention_start

    return max(retention_start, max_date - timedelta(days=overlap_days))


def _build_refresh_plan(
    *,
    pg_conn,
    table_names: list[str],
    retention_days: int,
    overlap_days: int,
    full: bool,
) -> dict[str, object]:
    refresh_starts: dict[str, object] = {}
    for table_name in table_names:
        refresh_starts[table_name] = _compute_refresh_start(
            pg_conn=pg_conn,
            pg_table=FEATURE_TABLES[table_name]["pg_table"],
            retention_days=retention_days,
            overlap_days=overlap_days,
            full=full,
        )

    earliest_refresh_start = min(refresh_starts.values())
    build_source_start = earliest_refresh_start - timedelta(days=SERVING_LOOKBACK_DAYS)
    build_history_start = build_source_start
    return {
        "refresh_starts": refresh_starts,
        "build_source_start": build_source_start,
        "build_history_start": build_history_start,
    }


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _parse_csv_values(raw_value: str) -> list[str]:
    if not raw_value:
        return []
    values = []
    seen = set()
    for item in raw_value.split(","):
        value = item.strip()
        if not value or value in seen:
            continue
        values.append(value)
        seen.add(value)
    return values


def _parse_domain_values(raw_value: str) -> list[int]:
    domains = []
    for value in _parse_csv_values(raw_value):
        try:
            domains.append(int(value))
        except ValueError as exc:
            raise ValueError(f"invalid THEME_FEATURE_TARGET_DOMAINS value: {value}") from exc
    return domains


def _sql_in_list(values: list[str]) -> str:
    return ", ".join(_sql_literal(value) for value in values)


def _target_condition(alias: str, target_asins: list[str], target_domains: list[int]) -> str:
    conditions = []
    if target_asins:
        conditions.append(f"{alias}.asin IN ({_sql_in_list(target_asins)})")
    if target_domains:
        conditions.append(f"{alias}.domain IN ({', '.join(str(domain) for domain in target_domains)})")
    return " AND ".join(conditions)


def _target_pg_where(target_asins: list[str], target_domains: list[int]) -> tuple[str, list[object]]:
    clauses = []
    params: list[object] = []
    if target_asins:
        clauses.append("asin = ANY(%s)")
        params.append(target_asins)
    if target_domains:
        clauses.append("domain = ANY(%s)")
        params.append(target_domains)
    if not clauses:
        return "", []
    return " AND " + " AND ".join(clauses), params


def build_serving_feature_tables(
    duck: duckdb.DuckDBPyConnection,
    *,
    build_source_start,
    build_history_start,
    target_asins: list[str],
    target_domains: list[int],
) -> None:
    target_history_condition = _target_condition("h", target_asins, target_domains)
    target_keyword_condition = _target_condition("m", target_asins, target_domains)
    target_history_filter = f"AND {target_history_condition}" if target_history_condition else ""
    target_keyword_filter = f"AND {target_keyword_condition}" if target_keyword_condition else ""
    target_keyword_where = f"WHERE {target_keyword_condition}" if target_keyword_condition else ""

    logger.info(
        "构建 DuckDB serving 基础特征临时表 (history_start=%s, source_start=%s)",
        build_history_start,
        build_source_start,
    )
    duck.execute(
        "CREATE OR REPLACE TEMP TABLE theme_sync_base_daily AS "
        + THEME_BASE_SOURCE_SQL.format(
            source_start_date=build_source_start.isoformat(),
            history_start_date=build_history_start.isoformat(),
            domain_multiplier_case=_domain_multiplier_case(),
            coeff_a_case=_category_coeff_case("coeff_a"),
            coeff_b_case=_category_coeff_case("coeff_b"),
            target_history_filter=target_history_filter,
        )
    )
    logger.info("DuckDB serving 基础特征临时表构建完成")

    logger.info("构建 DuckDB serving 趋势特征临时表 (source_start=%s)", build_source_start)
    duck.execute(
        "CREATE OR REPLACE TEMP TABLE theme_sync_trends_daily AS "
        + THEME_TRENDS_SOURCE_SQL.format(
            source_start_date=build_source_start.isoformat(),
            target_keyword_where=target_keyword_where,
            target_keyword_filter=target_keyword_filter,
        )
    )
    logger.info("DuckDB serving 趋势特征临时表构建完成")

    logger.info("构建 DuckDB serving 交叉特征临时表")
    duck.execute("CREATE OR REPLACE TEMP TABLE theme_sync_cross_daily AS " + THEME_CROSS_SOURCE_SQL)
    logger.info("DuckDB serving 交叉特征临时表构建完成")


def _export_subset_to_csv(
    duck: duckdb.DuckDBPyConnection,
    source_table: str,
    columns: list[str],
    start_date,
    export_path: Path,
) -> None:
    select_list = ",\n            ".join(columns)
    query = f"""
    COPY (
        SELECT
            {select_list}
        FROM {source_table}
        WHERE date >= DATE '{start_date.isoformat()}'
    ) TO {_sql_literal(export_path.as_posix())}
    WITH (FORMAT CSV, HEADER FALSE)
    """
    duck.execute(query)


def sync_feature_table(
    duck: duckdb.DuckDBPyConnection,
    pg_conn,
    table_name: str,
    config: dict,
    *,
    retention_days: int,
    full: bool,
    refresh_start,
    target_asins: list[str],
    target_domains: list[int],
) -> int:
    pg_table = config["pg_table"]
    source_table = config["source_table"]
    columns = config["columns"]
    retention_start = _retention_start_date(retention_days)

    logger.info(
        "同步 %s -> %s (refresh_start=%s, retention_start=%s, full=%s)",
        source_table,
        pg_table,
        refresh_start,
        retention_start,
        full,
    )

    temp_csv_path = Path(tempfile.NamedTemporaryFile(prefix=f"theme_{table_name}_", suffix=".csv", delete=False).name)
    temp_table_name = f"tmp_theme_{table_name}_{int(time.time() * 1000)}"
    column_list_sql = ", ".join(columns)

    try:
        _export_subset_to_csv(duck, source_table, columns, refresh_start, temp_csv_path)
        with pg_conn.cursor() as cur:
            cur.execute(f"CREATE TEMP TABLE {temp_table_name} (LIKE {pg_table} INCLUDING DEFAULTS) ON COMMIT DROP")
            with temp_csv_path.open("r", encoding="utf-8", newline="") as handle:
                cur.copy_expert(
                    f"COPY {temp_table_name} ({column_list_sql}) FROM STDIN WITH (FORMAT CSV)",
                    handle,
                )
            cur.execute(f"SELECT COUNT(*) FROM {temp_table_name}")
            copied_rows = int(cur.fetchone()[0])

            if full:
                if target_asins or target_domains:
                    target_where, target_params = _target_pg_where(target_asins, target_domains)
                    cur.execute(f"DELETE FROM {pg_table} WHERE TRUE {target_where}", target_params)
                else:
                    cur.execute(f"TRUNCATE TABLE {pg_table}")
            else:
                target_where, target_params = _target_pg_where(target_asins, target_domains)
                cur.execute(
                    f"DELETE FROM {pg_table} WHERE date >= %s {target_where}",
                    [refresh_start, *target_params],
                )

            cur.execute(
                f"INSERT INTO {pg_table} ({column_list_sql}) SELECT {column_list_sql} FROM {temp_table_name}"
            )
            cur.execute(f"DELETE FROM {pg_table} WHERE date < %s", [retention_start])

        pg_conn.commit()
        logger.info("  %s: %s rows refreshed", pg_table, copied_rows)
        return copied_rows
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        temp_csv_path.unlink(missing_ok=True)


def _parse_table_names(raw_tables: str | None) -> list[str]:
    if not raw_tables:
        return list(FEATURE_TABLES.keys())

    selected = []
    for item in raw_tables.split(","):
        name = item.strip().lower()
        if not name:
            continue
        if name not in FEATURE_TABLES:
            raise ValueError(f"unsupported table selection: {name}")
        selected.append(name)

    if not selected:
        raise ValueError("no feature tables selected")
    return selected


def run_sync(
    *,
    full: bool,
    retention_days: int,
    overlap_days: int,
    table_names: list[str],
    duckdb_path: str | Path,
) -> dict[str, int]:
    logger.info(
        "主题特征同步开始 — source=%s tables=%s retention=%sd overlap=%sd mode=%s",
        Path(duckdb_path).resolve(),
        ",".join(table_names),
        retention_days,
        overlap_days,
        "full" if full else "incremental",
    )
    started_at = time.time()
    results: dict[str, int] = {}
    target_asins = _parse_csv_values(THEME_FEATURE_TARGET_ASINS)
    target_domains = _parse_domain_values(THEME_FEATURE_TARGET_DOMAINS)
    if target_asins or target_domains:
        logger.info(
            "主题特征目标刷新 — asins=%s domains=%s",
            len(target_asins) if target_asins else "all",
            target_domains if target_domains else "all",
        )

    duck = get_duckdb_conn(duckdb_path)
    pg_conn = get_pg_conn()

    try:
        ensure_pg_schema(pg_conn)
        refresh_plan = _build_refresh_plan(
            pg_conn=pg_conn,
            table_names=table_names,
            retention_days=retention_days,
            overlap_days=overlap_days,
            full=full,
        )
        logger.info(
            "DuckDB 临时特征表构建窗口 — history_start=%s source_start=%s duckdb_threads=%s",
            refresh_plan["build_history_start"],
            refresh_plan["build_source_start"],
            DEFAULT_DUCKDB_THREADS,
        )
        build_serving_feature_tables(
            duck,
            build_source_start=refresh_plan["build_source_start"],
            build_history_start=refresh_plan["build_history_start"],
            target_asins=target_asins,
            target_domains=target_domains,
        )
        for table_name in table_names:
            try:
                results[table_name] = sync_feature_table(
                    duck,
                    pg_conn,
                    table_name,
                    FEATURE_TABLES[table_name],
                    retention_days=retention_days,
                    full=full,
                    refresh_start=refresh_plan["refresh_starts"][table_name],
                    target_asins=target_asins,
                    target_domains=target_domains,
                )
            except Exception as exc:
                logger.error("  同步 %s 失败: %s", table_name, exc)
                results[table_name] = -1
    finally:
        duck.close()
        while _SYNC_COPY_DIRS:
            shutil.rmtree(_SYNC_COPY_DIRS.pop(), ignore_errors=True)
        pg_conn.close()

    elapsed = round(time.time() - started_at, 1)
    total_rows = sum(value for value in results.values() if value > 0)
    logger.info("主题特征同步完成 — %s rows in %ss", total_rows, elapsed)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync theme serving feature subsets into PostgreSQL")
    parser.add_argument("--full", action="store_true", help="全量刷新 retention 窗口内数据")
    parser.add_argument("--loop", action="store_true", help="循环执行")
    parser.add_argument("--interval", type=int, default=86400, help="循环间隔秒数 (默认 86400)")
    parser.add_argument("--retention-days", type=int, default=DEFAULT_RETENTION_DAYS, help="在线 serving 保留天数")
    parser.add_argument("--overlap-days", type=int, default=DEFAULT_OVERLAP_DAYS, help="增量刷新回补窗口天数")
    parser.add_argument("--duckdb-path", type=str, default=str(DEFAULT_DUCKDB_PATH), help="DuckDB 源库路径")
    parser.add_argument("--tables", type=str, default=None, help="逗号分隔的表名: base,trends,cross")
    parser.add_argument("--lock-file", type=str, default=str(DEFAULT_LOCK_PATH), help="单实例锁文件路径")
    args = parser.parse_args()

    if args.retention_days < 30:
        sys.exit("retention-days must be >= 30")
    if args.overlap_days < 7:
        sys.exit("overlap-days must be >= 7")

    try:
        table_names = _parse_table_names(args.tables)
    except ValueError as exc:
        sys.exit(str(exc))

    lock_handle = acquire_process_lock(args.lock_file)
    try:
        if args.loop:
            logger.info("定时主题特征同步模式 — 间隔 %ss", args.interval)
            while True:
                try:
                    run_sync(
                        full=args.full,
                        retention_days=args.retention_days,
                        overlap_days=args.overlap_days,
                        table_names=table_names,
                        duckdb_path=args.duckdb_path,
                    )
                    args.full = False
                except Exception as exc:
                    logger.error("主题特征同步异常: %s", exc)
                time.sleep(args.interval)
        else:
            run_sync(
                full=args.full,
                retention_days=args.retention_days,
                overlap_days=args.overlap_days,
                table_names=table_names,
                duckdb_path=args.duckdb_path,
            )
    finally:
        lock_handle.close()


if __name__ == "__main__":
    main()