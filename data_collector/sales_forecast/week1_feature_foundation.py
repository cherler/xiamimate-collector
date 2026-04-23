from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
import json
import os
import shutil
import tempfile

import duckdb
from dotenv import load_dotenv

from ..cross_border_data.collectors.product import KEEPA_DOMAIN_TO_GEO
from .bsr_sales_converter import CATEGORY_COEFFICIENTS, DEFAULT_COEFFICIENTS, DOMAIN_MULTIPLIER


load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def _resolve_configured_path(env_name: str, default_path: Path) -> Path:
    configured = os.environ.get(env_name)
    if configured:
        return Path(configured).expanduser().resolve()
    return default_path.resolve()


def _resolve_int_env(env_name: str, default_value: int, *, minimum: int = 1) -> int:
    raw = os.environ.get(env_name)
    if not raw:
        return default_value
    try:
        return max(minimum, int(raw))
    except ValueError:
        return default_value


DEFAULT_SOURCE_DB = (
    _resolve_configured_path(
        "XIAMIMATE_DUCKDB_PATH",
        Path(__file__).resolve().parents[2]
        / "data_platform"
        / "storage"
        / "warehouse"
        / "local_analytics.duckdb",
    )
)
DEFAULT_DATA_PLATFORM_ROOT = _resolve_configured_path(
    "XIAMIMATE_DATA_PLATFORM_ROOT",
    Path("/Volumes/E/data/xiamimate-data-platform"),
)
DEFAULT_OUTPUT_DIR = (
    DEFAULT_DATA_PLATFORM_ROOT
    / "storage"
    / "features"
    / "training_sets"
    / "week1_foundation"
)
DEFAULT_TEMP_ROOT = _resolve_configured_path(
    "WEEK1_FOUNDATION_TEMP_ROOT",
    DEFAULT_DATA_PLATFORM_ROOT / "tmp" / "week1_foundation",
)

BASE_FEATURE_FILE = "features_base_daily.parquet"
TREND_FEATURE_FILE = "features_trends_daily.parquet"
CROSS_FEATURE_FILE = "features_cross_daily.parquet"
TRAINING_FEATURE_FILE = "training_dataset_daily.parquet"
QUALITY_REPORT_FILE = "feature_quality_report.md"
MANIFEST_FILE = "feature_build_manifest.json"
DEFAULT_DUCKDB_THREADS = _resolve_int_env("WEEK1_FOUNDATION_DUCKDB_THREADS", 1)
DEFAULT_DUCKDB_MEMORY_LIMIT = os.environ.get("WEEK1_FOUNDATION_DUCKDB_MEMORY_LIMIT", "16GB")
DEFAULT_FEATURE_PROFILE = os.environ.get("WEEK1_FOUNDATION_FEATURE_PROFILE", "full").strip().lower() or "full"

FEATURE_PROFILE_FULL = "full"
FEATURE_PROFILE_BASE = "base"
VALID_FEATURE_PROFILES = {FEATURE_PROFILE_FULL, FEATURE_PROFILE_BASE}


def _normalize_feature_profile(feature_profile: str | None) -> str:
    profile = (feature_profile or DEFAULT_FEATURE_PROFILE).strip().lower().replace("-", "_")
    alias_map = {
        "full": FEATURE_PROFILE_FULL,
        "base": FEATURE_PROFILE_BASE,
        "base_only": FEATURE_PROFILE_BASE,
    }
    normalized = alias_map.get(profile)
    if normalized is None:
        raise ValueError(
            f"Unsupported feature profile: {feature_profile}. Expected one of {sorted(VALID_FEATURE_PROFILES)}"
        )
    return normalized


def _progress_log(message: str) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [week1-foundation] {message}", flush=True)


def _count_rows(conn: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def _duckdb_wal_path(db_path: Path) -> Path:
    return Path(f"{db_path}.wal")


def discover_available_domains(
    source_db_path: str | Path | None = None,
    *,
    active_only: bool = False,
) -> list[int]:
    source_path = Path(source_db_path or DEFAULT_SOURCE_DB).resolve()
    try:
        conn = duckdb.connect(str(source_path), read_only=True)
        try:
            registry_where_clause = _build_registry_where_clause(domain=None, active_only=active_only)
            rows = conn.execute(
                f"""
                SELECT DISTINCT domain
                FROM curated.keepa_asin_registry
                {registry_where_clause}
                ORDER BY 1
                """
            ).fetchall()
        finally:
            conn.close()
        domains = [int(row[0]) for row in rows if row and row[0] is not None]
        if domains:
            return domains
    except duckdb.IOException:
        pass

    return sorted(KEEPA_DOMAIN_TO_GEO.keys())


def build_domain_output_dir(output_dir: str | Path | None, domain: int) -> Path:
    base_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    geo = KEEPA_DOMAIN_TO_GEO.get(domain, f"domain{domain}").lower()
    return base_output_dir / f"domain={domain}_{geo}"


def _domain_multiplier_case(domain_expr: str = "domain") -> str:
    when_clauses = "\n".join(
        f"WHEN {domain_expr} = {domain} THEN {multiplier}"
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


BASE_HISTORY_SQL = """
SELECT
    h.asin,
    h.domain,
    h.date,
    h.amazon_price,
    h.new_price,
    h.used_price,
    h.buy_box_price,
    h.list_price,
    h.bsr,
    h.rating,
    h.review_count,
    h.monthly_sold,
    h.new_offer_count,
    h.used_offer_count,
    ROW_NUMBER() OVER (PARTITION BY h.asin, h.domain ORDER BY h.date) AS history_row_number
FROM curated.keepa_product_history h
LEFT JOIN curated.keepa_asin_registry r
    ON h.asin = r.asin AND h.domain = r.domain
{where_clause}
"""

BASE_CORE_SQL = """
WITH min_date_cte AS (
    SELECT MIN(date) AS min_date
    FROM week1_base_history
),
filled_history AS (
    SELECT
        h.asin,
        h.domain,
        h.date,
        h.monthly_sold,
        h.history_row_number,
        LAST_VALUE(h.amazon_price IGNORE NULLS) OVER history_window AS amazon_price_ffill,
        LAST_VALUE(h.new_price IGNORE NULLS) OVER history_window AS new_price_ffill,
        LAST_VALUE(h.used_price IGNORE NULLS) OVER history_window AS used_price_ffill,
        LAST_VALUE(h.buy_box_price IGNORE NULLS) OVER history_window AS buy_box_price_ffill,
        LAST_VALUE(h.list_price IGNORE NULLS) OVER history_window AS list_price_ffill,
        LAST_VALUE(h.bsr IGNORE NULLS) OVER history_window AS bsr_ffill,
        LAST_VALUE(h.rating IGNORE NULLS) OVER history_window AS rating_ffill,
        LAST_VALUE(h.review_count IGNORE NULLS) OVER history_window AS review_count_ffill,
        LAST_VALUE(h.new_offer_count IGNORE NULLS) OVER history_window AS new_offer_count_ffill,
        LAST_VALUE(h.used_offer_count IGNORE NULLS) OVER history_window AS used_offer_count_ffill
    FROM week1_base_history h
    WINDOW history_window AS (
        PARTITION BY h.asin, h.domain
        ORDER BY h.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
)
SELECT
    h.asin,
    h.domain,
    CONCAT(CAST(h.domain AS VARCHAR), '::', h.asin) AS group_id,
    h.date,
    r.category_id,
    r.root_category_id,
    COALESCE(r.root_category_id, r.category_id, 0) AS root_category_key,
    COALESCE(r.is_active, TRUE) AS is_active,
    h.amazon_price_ffill AS amazon_price,
    h.new_price_ffill AS new_price,
    h.used_price_ffill AS used_price,
    h.buy_box_price_ffill AS buy_box_price,
    h.list_price_ffill AS list_price,
    h.bsr_ffill AS bsr,
    h.rating_ffill AS rating,
    h.review_count_ffill AS review_count,
    h.monthly_sold,
    h.new_offer_count_ffill AS new_offer_count,
    h.used_offer_count_ffill AS used_offer_count,
    COALESCE(h.buy_box_price_ffill, h.amazon_price_ffill, h.new_price_ffill) AS effective_price,
    CASE
        WHEN h.list_price_ffill > 0 AND COALESCE(h.buy_box_price_ffill, h.amazon_price_ffill, h.new_price_ffill) IS NOT NULL THEN
            ((h.list_price_ffill - COALESCE(h.buy_box_price_ffill, h.amazon_price_ffill, h.new_price_ffill)) / h.list_price_ffill) * 100
        ELSE NULL
    END AS price_discount_pct,
    CASE
        WHEN h.monthly_sold IS NOT NULL AND h.monthly_sold > 0 THEN h.monthly_sold / 30.0
        WHEN h.bsr_ffill IS NOT NULL AND h.bsr_ffill > 0 THEN ({domain_multiplier_case}) * ({coeff_a_case}) * POWER(CAST(h.bsr_ffill AS DOUBLE), {coeff_b_case})
        ELSE NULL
    END AS estimated_daily_sales,
    CASE
        WHEN h.monthly_sold IS NOT NULL AND h.monthly_sold > 0 THEN 'monthly_sold'
        WHEN h.bsr_ffill IS NOT NULL AND h.bsr_ffill > 0 THEN 'bsr_power_law'
        ELSE 'unavailable'
    END AS sales_estimation_method,
    LN(1 + GREATEST(COALESCE(h.bsr_ffill, 0), 0)) AS log_bsr,
    h.history_row_number,
    DATE_DIFF('day', (SELECT min_date FROM min_date_cte), h.date) AS time_idx,
    EXTRACT(ISODOW FROM h.date) - 1 AS day_of_week,
    EXTRACT(DAY FROM h.date) AS day_of_month,
    EXTRACT(WEEK FROM h.date) AS week_of_year,
    EXTRACT(MONTH FROM h.date) AS month,
    CASE WHEN EXTRACT(ISODOW FROM h.date) IN (6, 7) THEN 1 ELSE 0 END AS is_weekend
FROM filled_history h
LEFT JOIN curated.keepa_asin_registry r
    ON h.asin = r.asin AND h.domain = r.domain
"""

BASE_ENRICH_SQL = """
SELECT
    b.*,
    r.marketplace,
    r.product_title,
    r.brand,
    r.category,
    r.category_path
FROM {base_table_name} b
LEFT JOIN curated.keepa_asin_registry r
    ON b.asin = r.asin AND b.domain = r.domain
"""

BASE_LAG_SQL = """
WITH lag_seed AS (
    SELECT
        asin,
        domain,
        date,
        bsr,
        effective_price,
        review_count,
        estimated_daily_sales,
        LAG(bsr) OVER series_window AS bsr_lag_1,
        LAG(bsr, 7) OVER series_window AS bsr_lag_7,
        LAG(bsr, 14) OVER series_window AS bsr_lag_14,
        LAG(bsr, 30) OVER series_window AS bsr_lag_30,
        LAG(effective_price) OVER series_window AS effective_price_lag_1,
        LAG(effective_price, 7) OVER series_window AS effective_price_lag_7,
        LAG(effective_price, 14) OVER series_window AS effective_price_lag_14,
        LAG(effective_price, 30) OVER series_window AS effective_price_lag_30,
        LAG(review_count) OVER series_window AS review_count_lag_1,
        LAG(review_count, 7) OVER series_window AS review_count_lag_7,
        LAG(review_count, 14) OVER series_window AS review_count_lag_14,
        LAG(estimated_daily_sales) OVER series_window AS estimated_daily_sales_lag_1,
        LAG(estimated_daily_sales, 7) OVER series_window AS estimated_daily_sales_lag_7,
        LAG(estimated_daily_sales, 14) OVER series_window AS estimated_daily_sales_lag_14,
        LAG(estimated_daily_sales, 30) OVER series_window AS estimated_daily_sales_lag_30
    FROM week1_base_core
    WINDOW series_window AS (PARTITION BY asin, domain ORDER BY date)
)
SELECT
    asin,
    domain,
    date,
    bsr_lag_1,
    bsr_lag_7,
    bsr_lag_14,
    bsr_lag_30,
    effective_price_lag_1,
    effective_price_lag_7,
    effective_price_lag_14,
    effective_price_lag_30,
    review_count_lag_1,
    review_count_lag_7,
    review_count_lag_14,
    estimated_daily_sales_lag_1,
    estimated_daily_sales_lag_7,
    estimated_daily_sales_lag_14,
    estimated_daily_sales_lag_30,
    bsr - bsr_lag_1 AS bsr_change,
    CASE
        WHEN bsr_lag_1 > 0 THEN ((bsr - bsr_lag_1) / bsr_lag_1) * 100
        ELSE NULL
    END AS bsr_change_pct,
    effective_price - effective_price_lag_1 AS price_change,
    CASE
        WHEN effective_price_lag_1 > 0 THEN ((effective_price - effective_price_lag_1) / effective_price_lag_1) * 100
        ELSE NULL
    END AS price_change_pct,
    review_count - review_count_lag_1 AS review_velocity
FROM lag_seed
"""

BASE_ROLLING_SQL = """
SELECT
    asin,
    domain,
    date,
    AVG(estimated_daily_sales) OVER rolling_7_window AS estimated_daily_sales_roll_mean_7,
    AVG(estimated_daily_sales) OVER rolling_14_window AS estimated_daily_sales_roll_mean_14,
    AVG(estimated_daily_sales) OVER rolling_30_window AS estimated_daily_sales_roll_mean_30,
    STDDEV_SAMP(estimated_daily_sales) OVER rolling_7_window AS estimated_daily_sales_roll_std_7,
    AVG(bsr) OVER rolling_7_window AS bsr_roll_mean_7,
    AVG(bsr) OVER rolling_14_window AS bsr_roll_mean_14,
    AVG(bsr) OVER rolling_30_window AS bsr_roll_mean_30,
    STDDEV_SAMP(bsr) OVER rolling_7_window AS bsr_roll_std_7,
    AVG(effective_price) OVER rolling_7_window AS effective_price_roll_mean_7,
    AVG(effective_price) OVER rolling_14_window AS effective_price_roll_mean_14,
    AVG(review_count) OVER rolling_7_window AS review_count_roll_mean_7
FROM week1_base_core
WINDOW
    rolling_7_window AS (PARTITION BY asin, domain ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
    rolling_14_window AS (PARTITION BY asin, domain ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),
    rolling_30_window AS (PARTITION BY asin, domain ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
"""

BASE_DAILY_ASSEMBLY_SQL = """
SELECT
    c.*,
    l.bsr_lag_1,
    l.bsr_lag_7,
    l.bsr_lag_14,
    l.bsr_lag_30,
    l.effective_price_lag_1,
    l.effective_price_lag_7,
    l.effective_price_lag_14,
    l.effective_price_lag_30,
    l.review_count_lag_1,
    l.review_count_lag_7,
    l.review_count_lag_14,
    l.estimated_daily_sales_lag_1,
    l.estimated_daily_sales_lag_7,
    l.estimated_daily_sales_lag_14,
    l.estimated_daily_sales_lag_30,
    l.bsr_change,
    l.bsr_change_pct,
    l.price_change,
    l.price_change_pct,
    l.review_velocity,
    r.estimated_daily_sales_roll_mean_7,
    r.estimated_daily_sales_roll_mean_14,
    r.estimated_daily_sales_roll_mean_30,
    r.estimated_daily_sales_roll_std_7,
    r.bsr_roll_mean_7,
    r.bsr_roll_mean_14,
    r.bsr_roll_mean_30,
    r.bsr_roll_std_7,
    r.effective_price_roll_mean_7,
    r.effective_price_roll_mean_14,
    r.review_count_roll_mean_7
FROM week1_base_core c
LEFT JOIN week1_base_lag l
    ON c.asin = l.asin
   AND c.domain = l.domain
   AND c.date = l.date
LEFT JOIN week1_base_rolling r
    ON c.asin = r.asin
   AND c.domain = r.domain
   AND c.date = r.date
"""

TREND_FEATURE_SQL = """
WITH keyword_totals AS (
    SELECT asin, domain, COUNT(DISTINCT keyword) AS total_keywords
    FROM curated.asin_keyword_mapping
    {where_clause}
    GROUP BY 1, 2
),
trend_daily AS (
    SELECT
        m.asin,
        m.domain,
        t.date,
        COUNT(DISTINCT m.keyword) AS trend_keyword_hits,
        AVG(t.trend_index) AS trend_index_mean,
        MAX(t.trend_index) AS trend_index_max,
        MIN(t.trend_index) AS trend_index_min,
        AVG(t.search_volume) AS search_volume_mean
    FROM curated.asin_keyword_mapping m
    JOIN curated.google_trends_daily t
        ON m.keyword = t.keyword
       AND m.geo = t.geo
    {where_clause}
    GROUP BY 1, 2, 3
),
aligned AS (
    SELECT
        b.asin,
        b.domain,
        b.date,
        b.group_id,
        k.total_keywords,
        td.trend_keyword_hits,
        td.trend_index_mean,
        td.trend_index_max,
        td.trend_index_min,
        td.search_volume_mean
    FROM week1_base_daily b
    LEFT JOIN keyword_totals k
        ON b.asin = k.asin AND b.domain = k.domain
    LEFT JOIN trend_daily td
        ON b.asin = td.asin
       AND b.domain = td.domain
       AND b.date = td.date
)
SELECT
    *,
    LAG(trend_index_mean) OVER trend_window AS trend_index_lag_1,
    LAG(trend_index_mean, 7) OVER trend_window AS trend_index_lag_7,
    trend_index_mean - LAG(trend_index_mean) OVER trend_window AS trend_index_dod,
    trend_index_mean - LAG(trend_index_mean, 7) OVER trend_window AS trend_index_wow,
    AVG(trend_index_mean) OVER trend_roll_3 AS trend_index_roll_mean_3,
    AVG(trend_index_mean) OVER trend_roll_7 AS trend_index_roll_mean_7,
    STDDEV_SAMP(trend_index_mean) OVER trend_roll_7 AS trend_index_roll_std_7,
    MAX(trend_index_mean) OVER trend_roll_7 AS trend_index_roll_max_7,
    CASE
        WHEN AVG(trend_index_mean) OVER trend_roll_7 > 0 THEN trend_index_mean / AVG(trend_index_mean) OVER trend_roll_7
        ELSE NULL
    END AS trend_vs_roll_mean_ratio,
    CASE
        WHEN total_keywords > 0 THEN CAST(COALESCE(trend_keyword_hits, 0) AS DOUBLE) / total_keywords
        ELSE NULL
    END AS trend_keyword_coverage_ratio
FROM aligned
WINDOW
    trend_window AS (PARTITION BY asin, domain ORDER BY date),
    trend_roll_3 AS (PARTITION BY asin, domain ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
    trend_roll_7 AS (PARTITION BY asin, domain ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
"""

CROSS_FEATURE_SQL = """
WITH category_stats AS (
    SELECT
        domain,
        COALESCE(root_category_id, category_id, 0) AS root_category_key,
        COUNT(*) AS category_asin_count,
        COUNT(*) FILTER (WHERE COALESCE(is_active, TRUE)) AS category_active_asin_count
    FROM curated.keepa_asin_registry
    {where_clause}
    GROUP BY 1, 2
)
SELECT
    b.*, 
    t.total_keywords,
    t.trend_keyword_hits,
    t.trend_index_mean,
    t.trend_index_max,
    t.trend_index_min,
    t.search_volume_mean,
    t.trend_index_lag_1,
    t.trend_index_lag_7,
    t.trend_index_dod,
    t.trend_index_wow,
    t.trend_index_roll_mean_3,
    t.trend_index_roll_mean_7,
    t.trend_index_roll_std_7,
    t.trend_index_roll_max_7,
    t.trend_vs_roll_mean_ratio,
    t.trend_keyword_coverage_ratio,
    c.category_asin_count,
    c.category_active_asin_count,
    CASE
        WHEN b.effective_price IS NULL THEN NULL
        WHEN b.effective_price < 20 THEN '0_20'
        WHEN b.effective_price < 50 THEN '20_50'
        WHEN b.effective_price < 100 THEN '50_100'
        ELSE '100_plus'
    END AS price_band,
    CASE
        WHEN b.effective_price IS NULL THEN NULL
        WHEN b.effective_price < 20 THEN 1
        WHEN b.effective_price < 50 THEN 2
        WHEN b.effective_price < 100 THEN 3
        ELSE 4
    END AS price_band_index,
    COALESCE(t.trend_index_mean, 0) * COALESCE(b.effective_price, 0) AS trend_price_interaction,
    COALESCE(t.trend_index_mean, 0) * COALESCE(b.review_velocity, 0) AS trend_review_interaction,
    COALESCE(-b.bsr_change_pct, 0) * COALESCE(t.trend_index_dod, 0) AS bsr_trend_momentum,
    COALESCE(b.price_discount_pct, 0) * LN(1 + COALESCE(c.category_active_asin_count, 0)) AS discount_competition_interaction,
    COALESCE(t.trend_index_roll_mean_7, 0) * COALESCE(b.estimated_daily_sales_roll_mean_7, 0) AS trend_sales_interaction,
    COALESCE(b.review_velocity, 0) / NULLIF(COALESCE(b.review_count_lag_7, 0), 0) AS review_velocity_ratio
FROM week1_base_daily b
LEFT JOIN week1_trends_daily t
    ON b.asin = t.asin
   AND b.domain = t.domain
   AND b.date = t.date
LEFT JOIN category_stats c
    ON b.domain = c.domain
   AND b.root_category_key = c.root_category_key
"""

TRAINING_FEATURE_SQL = """
SELECT
    *,
    estimated_daily_sales AS target_sales
FROM week1_cross_daily
WHERE estimated_daily_sales IS NOT NULL
  AND history_row_number >= 3
"""


def _build_base_where_clause(*, domain: int | None, active_only: bool) -> str:
    conditions: list[str] = []
    if domain is not None:
        conditions.append(f"h.domain = {domain}")
    if active_only:
        conditions.append("COALESCE(r.is_active, TRUE)")
    if not conditions:
        return ""
    return "WHERE " + " AND ".join(conditions)


def _build_mapping_where_clause(*, domain: int | None, active_only: bool) -> str:
    conditions: list[str] = []
    if domain is not None:
        conditions.append(f"m.domain = {domain}")
    if active_only:
        conditions.append(
            "EXISTS (SELECT 1 FROM curated.keepa_asin_registry r WHERE r.asin = m.asin AND r.domain = m.domain AND COALESCE(r.is_active, TRUE))"
        )
    if not conditions:
        return ""
    return "WHERE " + " AND ".join(conditions)


def _build_registry_where_clause(*, domain: int | None, active_only: bool) -> str:
    conditions: list[str] = []
    if domain is not None:
        conditions.append(f"domain = {domain}")
    if active_only:
        conditions.append("COALESCE(is_active, TRUE)")
    if not conditions:
        return ""
    return "WHERE " + " AND ".join(conditions)


def build_week1_feature_tables(
    conn: duckdb.DuckDBPyConnection,
    *,
    domain: int | None = None,
    active_only: bool = False,
    include_training: bool = True,
    feature_profile: str = FEATURE_PROFILE_FULL,
    progress: Callable[[str], None] | None = None,
) -> None:
    progress = progress or (lambda _message: None)
    feature_profile = _normalize_feature_profile(feature_profile)
    base_where_clause = _build_base_where_clause(domain=domain, active_only=active_only)
    progress("building week1_base_history")
    conn.execute("CREATE OR REPLACE TEMP TABLE week1_base_history AS " + BASE_HISTORY_SQL.format(
        where_clause=base_where_clause,
    ))
    progress(f"built week1_base_history rows={_count_rows(conn, 'week1_base_history')}")
    progress("building week1_base_core")
    conn.execute("CREATE OR REPLACE TEMP TABLE week1_base_core AS " + BASE_CORE_SQL.format(
        domain_multiplier_case=_domain_multiplier_case("h.domain"),
        coeff_a_case=_category_coeff_case("coeff_a"),
        coeff_b_case=_category_coeff_case("coeff_b"),
    ))
    progress(f"built week1_base_core rows={_count_rows(conn, 'week1_base_core')}")
    conn.execute("DROP TABLE week1_base_history")
    progress("building week1_base_lag")
    conn.execute("CREATE OR REPLACE TEMP TABLE week1_base_lag AS " + BASE_LAG_SQL)
    progress(f"built week1_base_lag rows={_count_rows(conn, 'week1_base_lag')}")
    progress("building week1_base_rolling")
    conn.execute("CREATE OR REPLACE TEMP TABLE week1_base_rolling AS " + BASE_ROLLING_SQL)
    progress(f"built week1_base_rolling rows={_count_rows(conn, 'week1_base_rolling')}")
    progress("assembling week1_base_daily_metrics")
    conn.execute("CREATE OR REPLACE TEMP TABLE week1_base_daily_metrics AS " + BASE_DAILY_ASSEMBLY_SQL)
    progress(f"built week1_base_daily_metrics rows={_count_rows(conn, 'week1_base_daily_metrics')}")
    conn.execute("DROP TABLE week1_base_rolling")
    conn.execute("DROP TABLE week1_base_lag")
    conn.execute("DROP TABLE week1_base_core")
    progress("enriching week1_base_daily with registry metadata")
    conn.execute("CREATE OR REPLACE TEMP TABLE week1_base_daily AS " + BASE_ENRICH_SQL.format(
        base_table_name="week1_base_daily_metrics",
    ))
    progress(f"built week1_base_daily rows={_count_rows(conn, 'week1_base_daily')}")
    conn.execute("DROP TABLE week1_base_daily_metrics")

    if feature_profile == FEATURE_PROFILE_BASE:
        progress("feature_profile=base: skipping trends, cross, and training dataset stages")
        return

    mapping_where_clause = _build_mapping_where_clause(domain=domain, active_only=active_only)
    progress("building week1_trends_daily")
    conn.execute("CREATE OR REPLACE TEMP TABLE week1_trends_daily AS " + TREND_FEATURE_SQL.format(
        where_clause=mapping_where_clause,
    ))
    progress(f"built week1_trends_daily rows={_count_rows(conn, 'week1_trends_daily')}")

    registry_where_clause = _build_registry_where_clause(domain=domain, active_only=active_only)
    progress("building week1_cross_daily")
    conn.execute("CREATE OR REPLACE TEMP TABLE week1_cross_daily AS " + CROSS_FEATURE_SQL.format(
        where_clause=registry_where_clause,
    ))
    progress(f"built week1_cross_daily rows={_count_rows(conn, 'week1_cross_daily')}")

    if include_training:
        progress("building week1_training_dataset_daily")
        conn.execute("CREATE OR REPLACE TEMP TABLE week1_training_dataset_daily AS " + TRAINING_FEATURE_SQL)
        progress(f"built week1_training_dataset_daily rows={_count_rows(conn, 'week1_training_dataset_daily')}")


class Week1FeatureFoundationBuilder:
    def __init__(
        self,
        *,
        source_db_path: str | Path | None = None,
        output_dir: str | Path | None = None,
        domain: int | None = None,
        active_only: bool = False,
        duckdb_threads: int | None = None,
        feature_profile: str = FEATURE_PROFILE_FULL,
    ) -> None:
        self.source_db_path = Path(source_db_path or DEFAULT_SOURCE_DB).resolve()
        self.output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
        self.domain = domain
        self.active_only = active_only
        self.duckdb_threads = max(1, duckdb_threads or DEFAULT_DUCKDB_THREADS)
        self.feature_profile = _normalize_feature_profile(feature_profile)

    def build(self) -> dict[str, Path]:
        if not self.source_db_path.exists():
            raise FileNotFoundError(f"DuckDB source not found: {self.source_db_path}")

        self.output_dir.mkdir(parents=True, exist_ok=True)
        DEFAULT_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        self._log(
            "build start "
            f"source_db={self.source_db_path} output_dir={self.output_dir} "
            f"domain={self.domain if self.domain is not None else 'all'} active_only={self.active_only} "
            f"feature_profile={self.feature_profile} duckdb_threads={self.duckdb_threads}"
        )
        with tempfile.TemporaryDirectory(prefix="xiamimate_week1_", dir=DEFAULT_TEMP_ROOT) as temp_dir:
            snapshot_path = Path(temp_dir) / self.source_db_path.name
            self._create_snapshot(snapshot_path)

            conn = duckdb.connect(str(snapshot_path))
            try:
                self._log(
                    f"duckdb settings temp_directory={Path(temp_dir)} memory_limit={DEFAULT_DUCKDB_MEMORY_LIMIT} threads={self.duckdb_threads}"
                )
                conn.execute(f"SET temp_directory='{Path(temp_dir).as_posix()}'")
                conn.execute("SET preserve_insertion_order=false")
                conn.execute(f"SET memory_limit='{DEFAULT_DUCKDB_MEMORY_LIMIT}'")
                conn.execute(f"SET threads TO {self.duckdb_threads}")
                self._build_tables(conn)
                output_paths = self._write_parquet_outputs(conn)
                self._log("writing quality report")
                report_path = self._write_quality_report(conn)
                self._log(f"wrote quality report {report_path}")
                self._log("writing manifest")
                manifest_path = self._write_manifest(conn, output_paths | {"quality_report": report_path})
                self._log(f"wrote manifest {manifest_path}")
                output_paths["quality_report"] = report_path
                output_paths["manifest"] = manifest_path
                self._log("build completed")
                return output_paths
            finally:
                conn.close()

    def _build_tables(self, conn: duckdb.DuckDBPyConnection) -> None:
        build_week1_feature_tables(
            conn,
            domain=self.domain,
            active_only=self.active_only,
            include_training=self.feature_profile == FEATURE_PROFILE_FULL,
            feature_profile=self.feature_profile,
            progress=self._log,
        )

    def _log(self, message: str) -> None:
        domain_label = self.domain if self.domain is not None else "all"
        _progress_log(f"domain={domain_label} {message}")

    def _create_snapshot(self, snapshot_path: Path) -> None:
        snapshot_wal_path = _duckdb_wal_path(snapshot_path)
        source_wal_path = _duckdb_wal_path(self.source_db_path)
        self._log(f"copying source db to temp snapshot {snapshot_path}")
        shutil.copy2(self.source_db_path, snapshot_path)

        if source_wal_path.exists():
            try:
                shutil.copy2(source_wal_path, snapshot_wal_path)
                self._log(f"copied source wal to temp snapshot {snapshot_wal_path}")
            except FileNotFoundError:
                self._log(f"source wal disappeared during snapshot copy: {source_wal_path}")
        else:
            self._log("source wal not present; using main db snapshot only")

        self._log(
            "snapshot copy is read-only on the live DuckDB files; it should not block auto-collect writes, "
            "but it will add disk I/O while copying."
        )

    def _build_base_where_clause(self) -> str:
        return _build_base_where_clause(domain=self.domain, active_only=self.active_only)

    def _build_mapping_where_clause(self) -> str:
        return _build_mapping_where_clause(domain=self.domain, active_only=self.active_only)

    def _build_registry_where_clause(self) -> str:
        return _build_registry_where_clause(domain=self.domain, active_only=self.active_only)

    def _write_parquet_outputs(self, conn: duckdb.DuckDBPyConnection) -> dict[str, Path]:
        output_map = {
            "base_features": ("week1_base_daily", self.output_dir / BASE_FEATURE_FILE),
        }

        if self.feature_profile == FEATURE_PROFILE_FULL:
            output_map.update(
                {
                    "trend_features": ("week1_trends_daily", self.output_dir / TREND_FEATURE_FILE),
                    "cross_features": ("week1_cross_daily", self.output_dir / CROSS_FEATURE_FILE),
                    "training_dataset": ("week1_training_dataset_daily", self.output_dir / TRAINING_FEATURE_FILE),
                }
            )

        for table_name, output_path in output_map.values():
            row_count = _count_rows(conn, table_name)
            self._log(f"writing parquet table={table_name} rows={row_count} path={output_path}")
            conn.execute(
                f"COPY (SELECT * FROM {table_name}) TO '{output_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        return {name: path for name, (_, path) in output_map.items()}

    def _write_quality_report(self, conn: duckdb.DuckDBPyConnection) -> Path:
        report_path = self.output_dir / QUALITY_REPORT_FILE
        dataset_rows = []
        dataset_specs = [("基础特征", "week1_base_daily")]
        if self.feature_profile == FEATURE_PROFILE_FULL:
            dataset_specs.extend(
                [
                    ("趋势特征", "week1_trends_daily"),
                    ("交叉特征", "week1_cross_daily"),
                    ("训练数据集", "week1_training_dataset_daily"),
                ]
            )

        for dataset_name, table_name in dataset_specs:
            stats = conn.execute(
                f"""
                SELECT
                    COUNT(*) AS row_count,
                    COUNT(DISTINCT asin || '::' || CAST(domain AS VARCHAR)) AS asin_domain_count,
                    MIN(date) AS min_date,
                    MAX(date) AS max_date
                FROM {table_name}
                """
            ).fetchone()
            dataset_rows.append((dataset_name, stats))

        if self.feature_profile == FEATURE_PROFILE_FULL:
            key_nulls = conn.execute(
                """
                SELECT
                    ROUND(AVG(CASE WHEN effective_price IS NULL THEN 1.0 ELSE 0.0 END) * 100, 2) AS effective_price_null_pct,
                    ROUND(AVG(CASE WHEN bsr IS NULL THEN 1.0 ELSE 0.0 END) * 100, 2) AS bsr_null_pct,
                    ROUND(AVG(CASE WHEN estimated_daily_sales IS NULL THEN 1.0 ELSE 0.0 END) * 100, 2) AS estimated_daily_sales_null_pct,
                    ROUND(AVG(CASE WHEN trend_index_mean IS NULL THEN 1.0 ELSE 0.0 END) * 100, 2) AS trend_index_null_pct,
                    ROUND(AVG(CASE WHEN category_active_asin_count IS NULL THEN 1.0 ELSE 0.0 END) * 100, 2) AS category_competition_null_pct
                FROM week1_cross_daily
                """
            ).fetchone()

            sales_methods = conn.execute(
                """
                SELECT sales_estimation_method, COUNT(*) AS row_count
                FROM week1_cross_daily
                GROUP BY 1
                ORDER BY row_count DESC
                """
            ).fetchall()

            domains = conn.execute(
                """
                SELECT domain, COUNT(*) AS row_count, COUNT(DISTINCT asin) AS asin_count
                FROM week1_cross_daily
                GROUP BY 1
                ORDER BY 1
                """
            ).fetchall()
        else:
            key_nulls = conn.execute(
                """
                SELECT
                    ROUND(AVG(CASE WHEN effective_price IS NULL THEN 1.0 ELSE 0.0 END) * 100, 2) AS effective_price_null_pct,
                    ROUND(AVG(CASE WHEN bsr IS NULL THEN 1.0 ELSE 0.0 END) * 100, 2) AS bsr_null_pct,
                    ROUND(AVG(CASE WHEN estimated_daily_sales IS NULL THEN 1.0 ELSE 0.0 END) * 100, 2) AS estimated_daily_sales_null_pct
                FROM week1_base_daily
                """
            ).fetchone()

            sales_methods = conn.execute(
                """
                SELECT sales_estimation_method, COUNT(*) AS row_count
                FROM week1_base_daily
                GROUP BY 1
                ORDER BY row_count DESC
                """
            ).fetchall()

            domains = conn.execute(
                """
                SELECT domain, COUNT(*) AS row_count, COUNT(DISTINCT asin) AS asin_count
                FROM week1_base_daily
                GROUP BY 1
                ORDER BY 1
                """
            ).fetchall()

        lines = [
            "# Feature Quality Report",
            "",
            f"- built_at_utc: {datetime.now(timezone.utc).isoformat()}",
            f"- source_db: {self.source_db_path}",
            f"- output_dir: {self.output_dir}",
            f"- domain_filter: {self.domain if self.domain is not None else 'all'}",
            f"- active_only: {self.active_only}",
            f"- feature_profile: {self.feature_profile}",
            f"- duckdb_threads: {self.duckdb_threads}",
            "",
            "## Dataset Summary",
            "",
            "| 数据集 | 行数 | ASIN-Domain 数 | 最小日期 | 最大日期 |",
            "| --- | ---: | ---: | --- | --- |",
        ]
        for dataset_name, stats in dataset_rows:
            lines.append(
                f"| {dataset_name} | {stats[0]} | {stats[1]} | {stats[2]} | {stats[3]} |"
            )

        lines.extend([
            "",
            "## Key Null Rates",
            "",
            "| 字段 | 空值率 |",
            "| --- | ---: |",
            f"| effective_price | {key_nulls[0]}% |",
            f"| bsr | {key_nulls[1]}% |",
            f"| estimated_daily_sales | {key_nulls[2]}% |",
            "",
            "## Sales Estimation Methods",
            "",
            "| 方法 | 行数 |",
            "| --- | ---: |",
        ])

        if self.feature_profile == FEATURE_PROFILE_FULL:
            lines.insert(lines.index("## Sales Estimation Methods") - 1, f"| trend_index_mean | {key_nulls[3]}% |")
            lines.insert(lines.index("## Sales Estimation Methods") - 1, f"| category_active_asin_count | {key_nulls[4]}% |")
        else:
            lines.extend([
                "",
                "> feature_profile=base: 已跳过趋势特征、交叉特征与训练集导出，用于更快地产出基础时序特征。",
            ])

        for method, row_count in sales_methods:
            lines.append(f"| {method} | {row_count} |")

        lines.extend([
            "",
            "## Domain Coverage",
            "",
            "| Domain | 行数 | ASIN 数 |",
            "| --- | ---: | ---: |",
        ])
        for domain, row_count, asin_count in domains:
            lines.append(f"| {domain} | {row_count} | {asin_count} |")

        report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return report_path

    def _write_manifest(self, conn: duckdb.DuckDBPyConnection, outputs: dict[str, Path]) -> Path:
        manifest_path = self.output_dir / MANIFEST_FILE
        feature_table_name = "week1_cross_daily" if self.feature_profile == FEATURE_PROFILE_FULL else "week1_base_daily"
        feature_columns = [
            row[0]
            for row in conn.execute(f"DESCRIBE {feature_table_name}").fetchall()
        ]
        manifest = {
            "built_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_db": str(self.source_db_path),
            "output_dir": str(self.output_dir),
            "domain_filter": self.domain,
            "active_only": self.active_only,
            "feature_profile": self.feature_profile,
            "duckdb_threads": self.duckdb_threads,
            "feature_table": feature_table_name,
            "feature_columns": feature_columns,
            "feature_count": len(feature_columns),
            "outputs": {name: str(path) for name, path in outputs.items()},
        }
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return manifest_path
