from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import os
import shutil
import tempfile

import duckdb
from dotenv import load_dotenv

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
    r.marketplace,
    r.product_title,
    r.brand,
    r.category,
    r.category_id,
    r.root_category_id,
    r.category_path,
    r.is_active,
    COALESCE(r.root_category_id, r.category_id, 0) AS root_category_key,
    LAST_VALUE(h.amazon_price IGNORE NULLS) OVER history_window AS amazon_price_ffill,
    LAST_VALUE(h.new_price IGNORE NULLS) OVER history_window AS new_price_ffill,
    LAST_VALUE(h.used_price IGNORE NULLS) OVER history_window AS used_price_ffill,
    LAST_VALUE(h.buy_box_price IGNORE NULLS) OVER history_window AS buy_box_price_ffill,
    LAST_VALUE(h.list_price IGNORE NULLS) OVER history_window AS list_price_ffill,
    LAST_VALUE(h.bsr IGNORE NULLS) OVER history_window AS bsr_ffill,
    LAST_VALUE(h.rating IGNORE NULLS) OVER history_window AS rating_ffill,
    LAST_VALUE(h.review_count IGNORE NULLS) OVER history_window AS review_count_ffill,
    LAST_VALUE(h.new_offer_count IGNORE NULLS) OVER history_window AS new_offer_count_ffill,
    LAST_VALUE(h.used_offer_count IGNORE NULLS) OVER history_window AS used_offer_count_ffill,
    ROW_NUMBER() OVER (PARTITION BY h.asin, h.domain ORDER BY h.date) AS history_row_number
FROM curated.keepa_product_history h
LEFT JOIN curated.keepa_asin_registry r
    ON h.asin = r.asin AND h.domain = r.domain
{where_clause}
WINDOW history_window AS (
    PARTITION BY h.asin, h.domain
    ORDER BY h.date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
"""

BASE_CORE_SQL = """
WITH min_date_cte AS (
    SELECT MIN(date) AS min_date
    FROM week1_base_history
)
SELECT
    asin,
    domain,
    CONCAT(CAST(domain AS VARCHAR), '::', asin) AS group_id,
    date,
    marketplace,
    product_title,
    brand,
    category,
    category_id,
    root_category_id,
    root_category_key,
    category_path,
    COALESCE(is_active, TRUE) AS is_active,
    amazon_price_ffill AS amazon_price,
    new_price_ffill AS new_price,
    used_price_ffill AS used_price,
    buy_box_price_ffill AS buy_box_price,
    list_price_ffill AS list_price,
    bsr_ffill AS bsr,
    rating_ffill AS rating,
    review_count_ffill AS review_count,
    monthly_sold,
    new_offer_count_ffill AS new_offer_count,
    used_offer_count_ffill AS used_offer_count,
    COALESCE(buy_box_price_ffill, amazon_price_ffill, new_price_ffill) AS effective_price,
    CASE
        WHEN list_price_ffill > 0 AND COALESCE(buy_box_price_ffill, amazon_price_ffill, new_price_ffill) IS NOT NULL THEN
            ((list_price_ffill - COALESCE(buy_box_price_ffill, amazon_price_ffill, new_price_ffill)) / list_price_ffill) * 100
        ELSE NULL
    END AS price_discount_pct,
    CASE
        WHEN monthly_sold IS NOT NULL AND monthly_sold > 0 THEN monthly_sold / 30.0
        WHEN bsr_ffill IS NOT NULL AND bsr_ffill > 0 THEN ({domain_multiplier_case}) * ({coeff_a_case}) * POWER(CAST(bsr_ffill AS DOUBLE), {coeff_b_case})
        ELSE NULL
    END AS estimated_daily_sales,
    CASE
        WHEN monthly_sold IS NOT NULL AND monthly_sold > 0 THEN 'monthly_sold'
        WHEN bsr_ffill IS NOT NULL AND bsr_ffill > 0 THEN 'bsr_power_law'
        ELSE 'unavailable'
    END AS sales_estimation_method,
    LN(1 + GREATEST(COALESCE(bsr_ffill, 0), 0)) AS log_bsr,
    history_row_number,
    DATE_DIFF('day', (SELECT min_date FROM min_date_cte), date) AS time_idx,
    EXTRACT(ISODOW FROM date) - 1 AS day_of_week,
    EXTRACT(DAY FROM date) AS day_of_month,
    EXTRACT(WEEK FROM date) AS week_of_year,
    EXTRACT(MONTH FROM date) AS month,
    CASE WHEN EXTRACT(ISODOW FROM date) IN (6, 7) THEN 1 ELSE 0 END AS is_weekend
FROM week1_base_history
"""

BASE_LAG_SQL = """
SELECT
    *,
    bsr - LAG(bsr) OVER series_window AS bsr_change,
    CASE
        WHEN LAG(bsr) OVER series_window > 0 THEN ((bsr - LAG(bsr) OVER series_window) / LAG(bsr) OVER series_window) * 100
        ELSE NULL
    END AS bsr_change_pct,
    effective_price - LAG(effective_price) OVER series_window AS price_change,
    CASE
        WHEN LAG(effective_price) OVER series_window > 0 THEN ((effective_price - LAG(effective_price) OVER series_window) / LAG(effective_price) OVER series_window) * 100
        ELSE NULL
    END AS price_change_pct,
    review_count - LAG(review_count) OVER series_window AS review_velocity,
    LAG(estimated_daily_sales) OVER series_window AS estimated_daily_sales_lag_1,
    LAG(estimated_daily_sales, 7) OVER series_window AS estimated_daily_sales_lag_7,
    LAG(estimated_daily_sales, 14) OVER series_window AS estimated_daily_sales_lag_14,
    LAG(estimated_daily_sales, 30) OVER series_window AS estimated_daily_sales_lag_30,
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
    LAG(review_count, 14) OVER series_window AS review_count_lag_14
FROM week1_base_core
WINDOW series_window AS (PARTITION BY asin, domain ORDER BY date)
"""

BASE_ROLLING_SQL = """
SELECT
    *,
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
FROM week1_base_lag
WINDOW
    rolling_7_window AS (PARTITION BY asin, domain ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
    rolling_14_window AS (PARTITION BY asin, domain ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),
    rolling_30_window AS (PARTITION BY asin, domain ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
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
) -> None:
    base_where_clause = _build_base_where_clause(domain=domain, active_only=active_only)
    conn.execute("CREATE OR REPLACE TEMP TABLE week1_base_history AS " + BASE_HISTORY_SQL.format(
        where_clause=base_where_clause,
    ))
    conn.execute("CREATE OR REPLACE TEMP TABLE week1_base_core AS " + BASE_CORE_SQL.format(
        domain_multiplier_case=_domain_multiplier_case(),
        coeff_a_case=_category_coeff_case("coeff_a"),
        coeff_b_case=_category_coeff_case("coeff_b"),
    ))
    conn.execute("DROP TABLE week1_base_history")
    conn.execute("CREATE OR REPLACE TEMP TABLE week1_base_lag AS " + BASE_LAG_SQL)
    conn.execute("DROP TABLE week1_base_core")
    conn.execute("CREATE OR REPLACE TEMP TABLE week1_base_daily AS " + BASE_ROLLING_SQL)
    conn.execute("DROP TABLE week1_base_lag")

    mapping_where_clause = _build_mapping_where_clause(domain=domain, active_only=active_only)
    conn.execute("CREATE OR REPLACE TEMP TABLE week1_trends_daily AS " + TREND_FEATURE_SQL.format(
        where_clause=mapping_where_clause,
    ))

    registry_where_clause = _build_registry_where_clause(domain=domain, active_only=active_only)
    conn.execute("CREATE OR REPLACE TEMP TABLE week1_cross_daily AS " + CROSS_FEATURE_SQL.format(
        where_clause=registry_where_clause,
    ))

    if include_training:
        conn.execute("CREATE OR REPLACE TEMP TABLE week1_training_dataset_daily AS " + TRAINING_FEATURE_SQL)


class Week1FeatureFoundationBuilder:
    def __init__(
        self,
        *,
        source_db_path: str | Path | None = None,
        output_dir: str | Path | None = None,
        domain: int | None = None,
        active_only: bool = False,
    ) -> None:
        self.source_db_path = Path(source_db_path or DEFAULT_SOURCE_DB).resolve()
        self.output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
        self.domain = domain
        self.active_only = active_only

    def build(self) -> dict[str, Path]:
        if not self.source_db_path.exists():
            raise FileNotFoundError(f"DuckDB source not found: {self.source_db_path}")

        self.output_dir.mkdir(parents=True, exist_ok=True)
        DEFAULT_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="xiamimate_week1_", dir=DEFAULT_TEMP_ROOT) as temp_dir:
            snapshot_path = Path(temp_dir) / self.source_db_path.name
            shutil.copy2(self.source_db_path, snapshot_path)

            conn = duckdb.connect(str(snapshot_path))
            try:
                conn.execute(f"SET temp_directory='{Path(temp_dir).as_posix()}'")
                conn.execute("SET preserve_insertion_order=false")
                conn.execute(f"SET memory_limit='{DEFAULT_DUCKDB_MEMORY_LIMIT}'")
                conn.execute(f"SET threads TO {DEFAULT_DUCKDB_THREADS}")
                self._build_tables(conn)
                output_paths = self._write_parquet_outputs(conn)
                report_path = self._write_quality_report(conn)
                manifest_path = self._write_manifest(conn, output_paths | {"quality_report": report_path})
                output_paths["quality_report"] = report_path
                output_paths["manifest"] = manifest_path
                return output_paths
            finally:
                conn.close()

    def _build_tables(self, conn: duckdb.DuckDBPyConnection) -> None:
        build_week1_feature_tables(
            conn,
            domain=self.domain,
            active_only=self.active_only,
            include_training=True,
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
            "trend_features": ("week1_trends_daily", self.output_dir / TREND_FEATURE_FILE),
            "cross_features": ("week1_cross_daily", self.output_dir / CROSS_FEATURE_FILE),
            "training_dataset": ("week1_training_dataset_daily", self.output_dir / TRAINING_FEATURE_FILE),
        }

        for table_name, output_path in output_map.values():
            conn.execute(
                f"COPY (SELECT * FROM {table_name}) TO '{output_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        return {name: path for name, (_, path) in output_map.items()}

    def _write_quality_report(self, conn: duckdb.DuckDBPyConnection) -> Path:
        report_path = self.output_dir / QUALITY_REPORT_FILE
        dataset_rows = []
        for dataset_name, table_name in [
            ("基础特征", "week1_base_daily"),
            ("趋势特征", "week1_trends_daily"),
            ("交叉特征", "week1_cross_daily"),
            ("训练数据集", "week1_training_dataset_daily"),
        ]:
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

        lines = [
            "# Feature Quality Report",
            "",
            f"- built_at_utc: {datetime.now(timezone.utc).isoformat()}",
            f"- source_db: {self.source_db_path}",
            f"- output_dir: {self.output_dir}",
            f"- domain_filter: {self.domain if self.domain is not None else 'all'}",
            f"- active_only: {self.active_only}",
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
            f"| trend_index_mean | {key_nulls[3]}% |",
            f"| category_active_asin_count | {key_nulls[4]}% |",
            "",
            "## Sales Estimation Methods",
            "",
            "| 方法 | 行数 |",
            "| --- | ---: |",
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
        feature_columns = [
            row[0]
            for row in conn.execute("DESCRIBE week1_cross_daily").fetchall()
        ]
        manifest = {
            "built_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_db": str(self.source_db_path),
            "output_dir": str(self.output_dir),
            "domain_filter": self.domain,
            "active_only": self.active_only,
            "feature_columns": feature_columns,
            "feature_count": len(feature_columns),
            "outputs": {name: str(path) for name, path in outputs.items()},
        }
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return manifest_path
