"""DuckDB 存储层.

将采集到的 Keepa / Google Trends 数据落地到 DuckDB, 并支持去重.

存储策略:
1. CSV 先写入 data_platform/storage/raw/json/ (原始备份)
2. 规范化后写入 Parquet → data_platform/storage/curated/parquet/
3. DuckDB 作为分析层, 从 Parquet 加载或直接 INSERT

表结构:
- curated.keepa_product_history  — 日粒度价格/BSR/评论时间序列
- curated.keepa_product_snapshot — 商品元数据快照 (最新一次)
- curated.keepa_asin_registry   — 已发现的 ASIN 注册表 (去重核心)
- curated.google_trends_daily   — Google Trends 搜索热度
- curated.collection_log        — 采集任务日志
- curated.asin_raw_file_mapping — ASIN 对应原始文件路径映射
"""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Any
import json

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore[assignment]

try:
    import pandas as pd
except ImportError:
    pd = None  # type: ignore[assignment]

# Keepa domain → Google Trends geo 映射 (同 collectors/product.py 保持一致)
_DOMAIN_TO_GEO = {
    1: "US", 2: "GB", 3: "DE", 4: "FR", 5: "JP",
    6: "CA", 8: "IT", 9: "ES", 10: "IN", 11: "MX", 12: "BR", 13: "AU",
}

_DOMAIN_PRICE_BANDS = {
    1: (15.0, 60.0),
    2: (12.0, 50.0),
    3: (15.0, 60.0),
    4: (15.0, 60.0),
    5: (2000.0, 8000.0),
    6: (20.0, 80.0),
    8: (15.0, 60.0),
    9: (15.0, 60.0),
    10: (1200.0, 5000.0),
    11: (300.0, 1200.0),
    12: (80.0, 300.0),
    13: (25.0, 90.0),
}

_BUSINESS_TIER_REFRESH_WINDOWS_DAYS = {
    "P0": (3, 7),
    "P1": (10, 14),
    "P2": (21, 30),
}

_BUSINESS_TIER_PRIORITY_WINDOWS = {
    "P0": (90, 100),
    "P1": (60, 80),
    "P2": (20, 40),
}


def _build_dynamic_stale_hours_sql(default_stale_hours: int) -> str:
    cases: list[str] = []
    for tier, (min_days, max_days) in _BUSINESS_TIER_REFRESH_WINDOWS_DAYS.items():
        priority_min, priority_max = _BUSINESS_TIER_PRIORITY_WINDOWS[tier]
        min_hours = min_days * 24
        max_hours = max_days * 24
        cases.append(
            f"""WHEN business_tier = '{tier}' THEN CAST(ROUND(
                    {max_hours} - (
                        (LEAST(GREATEST(COALESCE(business_priority, {priority_min}), {priority_min}), {priority_max}) - {priority_min})
                        * ({max_hours} - {min_hours})::DOUBLE / {priority_max - priority_min}
                    )
                ) AS BIGINT)"""
        )
    return (
        "CASE\n"
        + "\n".join(cases)
        + f"\nELSE {int(default_stale_hours)}\nEND"
    )


# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

DUCKDB_SCHEMA_DDL = """
CREATE SCHEMA IF NOT EXISTS curated;

-- ASIN 注册表: 所有已知 ASIN 的中心记录, 去重的核心
CREATE TABLE IF NOT EXISTS curated.keepa_asin_registry (
    asin              VARCHAR NOT NULL,
    domain            INTEGER NOT NULL DEFAULT 1,
    marketplace       VARCHAR,
    product_title     VARCHAR,
    brand             VARCHAR,
    category          VARCHAR,
    category_id       BIGINT,
    category_path     VARCHAR,           -- 完整类目路径: "Home & Kitchen > Kitchen & Dining > Coffee Makers"
    root_category_id  BIGINT,            -- L1 根类目 ID (来自 categoryTree)
    discovery_source  VARCHAR,       -- 'bestseller' / 'search' / 'seed' / 'manual'
    search_term       VARCHAR,
    priority          INTEGER DEFAULT 0,
    business_score_total INTEGER,
    business_tier     VARCHAR,
    business_priority INTEGER,
    score_updated_at  TIMESTAMP,
    first_seen_at     TIMESTAMP,
    last_fetched_at   TIMESTAMP,     -- 上次采集历史数据的时间
    last_snapshot_at  TIMESTAMP,     -- 上次采集快照的时间
    fetch_count       INTEGER DEFAULT 0,
    is_active         BOOLEAN DEFAULT TRUE,
    inactive_reason   VARCHAR,           -- 自动停用原因: 'no_data' / 'all_prices_null' / 'stale_90d' / 'fetch_failed_3x' / 'manual'
    inactive_at       TIMESTAMP,         -- 停用时间
    notes             VARCHAR,
    PRIMARY KEY (asin, domain)
);

-- Keepa 商品历史数据 (日粒度)
CREATE TABLE IF NOT EXISTS curated.keepa_product_history (
    asin              VARCHAR NOT NULL,
    domain            INTEGER NOT NULL DEFAULT 1,
    date              DATE NOT NULL,
    amazon_price      DOUBLE,
    new_price         DOUBLE,
    used_price        DOUBLE,
    buy_box_price     DOUBLE,
    list_price        DOUBLE,
    bsr               BIGINT,
    rating            DOUBLE,
    review_count      BIGINT,
    monthly_sold      BIGINT,
    new_offer_count   INTEGER,
    used_offer_count  INTEGER,
    ingested_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (asin, domain, date)
);

-- Keepa 商品快照 (每个 ASIN 保留最新一次)
CREATE TABLE IF NOT EXISTS curated.keepa_product_snapshot (
    asin                  VARCHAR NOT NULL,
    domain                INTEGER NOT NULL DEFAULT 1,
    marketplace           VARCHAR,
    product_title         VARCHAR,
    brand                 VARCHAR,
    category              VARCHAR,
    price                 DOUBLE,
    list_price            DOUBLE,
    bsr                   BIGINT,
    rating                DOUBLE,
    review_count          BIGINT,
    estimated_sales       BIGINT,
    estimated_sales_period VARCHAR,
    seller_count          INTEGER,
    total_offer_count     INTEGER,
    offer_count_fba       INTEGER,
    offer_count_fbm       INTEGER,
    retrieved_offer_count INTEGER,
    offers_successful     BOOLEAN,
    stock_status          VARCHAR,
    data_capture_time     TIMESTAMP,
    source_url            VARCHAR,
    keepa_last_update     BIGINT,
    ingested_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (asin, domain)
);

-- Google Trends 搜索热度
CREATE TABLE IF NOT EXISTS curated.google_trends_daily (
    keyword           VARCHAR NOT NULL,
    geo               VARCHAR NOT NULL DEFAULT 'GLOBAL',
    date              DATE NOT NULL,
    trend_index       DOUBLE,
    search_volume     DOUBLE,
    is_partial        BOOLEAN DEFAULT FALSE,
    ingested_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (keyword, geo, date)
);

-- 采集任务日志
CREATE TABLE IF NOT EXISTS curated.collection_log (
    job_id            INTEGER,
    run_date          DATE NOT NULL DEFAULT CURRENT_DATE,
    source            VARCHAR NOT NULL,   -- 'keepa_history' / 'keepa_snapshot' / 'google_trends' / 'bestsellers'
    domain            INTEGER,
    asins_requested   INTEGER DEFAULT 0,
    asins_succeeded   INTEGER DEFAULT 0,
    rows_ingested     INTEGER DEFAULT 0,
    tokens_before     INTEGER,
    tokens_after      INTEGER,
    tokens_consumed   INTEGER,
    duration_seconds  DOUBLE,
    raw_file_path     VARCHAR,
    error_message     VARCHAR,
    started_at        TIMESTAMP,
    finished_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ASIN ↔ 关键词映射: 连接 keepa_product_history 和 google_trends_daily 的桥表
CREATE TABLE IF NOT EXISTS curated.asin_keyword_mapping (
    asin              VARCHAR NOT NULL,
    domain            INTEGER NOT NULL DEFAULT 1,
    keyword           VARCHAR NOT NULL,
    geo               VARCHAR DEFAULT 'US',               -- Google Trends 地域代码, 由 domain 映射
    source            VARCHAR DEFAULT 'title_extract',  -- 'title_extract' / 'manual' / 'search_term'
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (asin, domain, keyword)
);

-- ASIN ↔ 原始文件映射: 便于按 ASIN 反查对应原始 JSON/GZIP 文件
CREATE TABLE IF NOT EXISTS curated.asin_raw_file_mapping (
    asin              VARCHAR NOT NULL,
    domain            INTEGER NOT NULL DEFAULT 1,
    source            VARCHAR NOT NULL,
    raw_file_path     VARCHAR NOT NULL,
    file_format       VARCHAR DEFAULT 'json',
    is_compressed     BOOLEAN DEFAULT FALSE,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (asin, domain, raw_file_path)
);

-- 下一阶段自动扩张状态: keyword / subcategory 的最近执行记录
CREATE TABLE IF NOT EXISTS curated.discovery_expansion_state (
    expansion_type       VARCHAR NOT NULL,
    domain               INTEGER NOT NULL DEFAULT 1,
    target_key           VARCHAR NOT NULL,
    target_label         VARCHAR,
    last_priority_score  DOUBLE,
    last_candidate_count INTEGER DEFAULT 0,
    last_new_asin_count  INTEGER DEFAULT 0,
    total_new_asin_count INTEGER DEFAULT 0,
    run_count            INTEGER DEFAULT 0,
    first_run_at         TIMESTAMP,
    last_run_at          TIMESTAMP,
    notes                VARCHAR,
    PRIMARY KEY (expansion_type, domain, target_key)
);

-- 类目注册表: 跟踪每个类目的 BestSeller 采集状态
CREATE TABLE IF NOT EXISTS curated.keepa_category_registry (
    category_id       BIGINT NOT NULL,
    domain            INTEGER NOT NULL DEFAULT 1,
    category_en       VARCHAR,
    category_cn       VARCHAR,
    parent_id         BIGINT,
    level             VARCHAR,
    product_count     BIGINT DEFAULT 0,
    depth             INTEGER DEFAULT 1,       -- 层级深度: 1=L1, 2=L2, 3=L3
    is_active         BOOLEAN DEFAULT TRUE,    -- 是否参与轮换
    bestseller_fetched_at  TIMESTAMP,          -- 上次读取 BestSeller 的时间
    bestseller_asin_count  INTEGER DEFAULT 0,  -- 上次读取到的 ASIN 数量
    children_fetched_at    TIMESTAMP,          -- 上次拉取子类目的时间
    sort_order        INTEGER DEFAULT 0,       -- CSV 中的顺序 (越小越优先)
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (category_id, domain)
);
"""


class DuckDBStorage:
    """DuckDB 存储管理器."""

    def __init__(self, db_path: str | Path | None = None) -> None:
        """
        Parameters
        ----------
        db_path : str or Path, optional
            DuckDB 文件路径. 如果不指定, 使用默认路径:
            data_platform/storage/warehouse/local_analytics.duckdb
        """
        if duckdb is None:
            raise ImportError(
                "DuckDB is required. Install it with: pip install duckdb"
            )

        if db_path is None:
            configured_db_path = os.environ.get("XIAMIMATE_DUCKDB_PATH") or os.environ.get("DUCKDB_PATH")
            if configured_db_path:
                db_path = Path(configured_db_path).expanduser()
            else:
                db_path = (
                    Path(__file__).resolve().parents[2]
                    / "data_platform"
                    / "storage"
                    / "warehouse"
                    / "local_analytics.duckdb"
                )
        self.db_path = Path(db_path).resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: duckdb.DuckDBPyConnection | None = None

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            self._conn = duckdb.connect(str(self.db_path))
            self._conn.execute(DUCKDB_SCHEMA_DDL)
            self._migrate(self._conn)
        return self._conn

    @staticmethod
    def _migrate(conn: duckdb.DuckDBPyConnection) -> None:
        """补齐早期建表时缺少的列."""
        _add_asin = (
            "ALTER TABLE curated.keepa_asin_registry ADD COLUMN IF NOT EXISTS"
        )
        conn.execute(f"{_add_asin} inactive_reason VARCHAR")
        conn.execute(f"{_add_asin} inactive_at TIMESTAMP")
        conn.execute(f"{_add_asin} category_path VARCHAR")
        conn.execute(f"{_add_asin} root_category_id BIGINT")
        conn.execute(f"{_add_asin} business_score_total INTEGER")
        conn.execute(f"{_add_asin} business_tier VARCHAR")
        conn.execute(f"{_add_asin} business_priority INTEGER")
        conn.execute(f"{_add_asin} score_updated_at TIMESTAMP")

        # category_id: INTEGER → BIGINT (Amazon catId 如 7141123011 超出 INT32)
        col_type = conn.execute("""
            SELECT data_type FROM information_schema.columns
            WHERE table_schema = 'curated'
              AND table_name = 'keepa_asin_registry'
              AND column_name = 'category_id'
        """).fetchone()
        if col_type and col_type[0] == 'INTEGER':
            conn.execute("""
                ALTER TABLE curated.keepa_asin_registry
                ADD COLUMN IF NOT EXISTS category_id_new BIGINT
            """)
            conn.execute("""
                UPDATE curated.keepa_asin_registry
                SET category_id_new = category_id::BIGINT
            """)
            conn.execute("ALTER TABLE curated.keepa_asin_registry DROP COLUMN category_id")
            conn.execute("ALTER TABLE curated.keepa_asin_registry RENAME COLUMN category_id_new TO category_id")

        _add_cat = (
            "ALTER TABLE curated.keepa_category_registry ADD COLUMN IF NOT EXISTS"
        )
        conn.execute(f"{_add_cat} depth INTEGER DEFAULT 1")
        conn.execute(f"{_add_cat} children_fetched_at TIMESTAMP")

        # asin_keyword_mapping: 补齐 geo 列
        conn.execute(
            "ALTER TABLE curated.asin_keyword_mapping "
            "ADD COLUMN IF NOT EXISTS geo VARCHAR DEFAULT 'US'"
        )

        # 历史数据中 stale_30d → stale_90d 重命名 (阈值改为 90 天)
        conn.execute(
            "UPDATE curated.keepa_asin_registry "
            "SET inactive_reason = 'stale_90d' "
            "WHERE inactive_reason = 'stale_30d'"
        )

        _add_snapshot = (
            "ALTER TABLE curated.keepa_product_snapshot ADD COLUMN IF NOT EXISTS"
        )
        conn.execute(f"{_add_snapshot} seller_count INTEGER")
        conn.execute(f"{_add_snapshot} total_offer_count INTEGER")
        conn.execute(f"{_add_snapshot} offer_count_fba INTEGER")
        conn.execute(f"{_add_snapshot} offer_count_fbm INTEGER")
        conn.execute(f"{_add_snapshot} retrieved_offer_count INTEGER")
        conn.execute(f"{_add_snapshot} offers_successful BOOLEAN")
        conn.execute(f"{_add_snapshot} keepa_last_update BIGINT")

        conn.execute(
            "ALTER TABLE curated.collection_log "
            "ADD COLUMN IF NOT EXISTS raw_file_path VARCHAR"
        )

        conn.execute(
            """CREATE TABLE IF NOT EXISTS curated.asin_raw_file_mapping (
                   asin          VARCHAR NOT NULL,
                   domain        INTEGER NOT NULL DEFAULT 1,
                   source        VARCHAR NOT NULL,
                   raw_file_path VARCHAR NOT NULL,
                   file_format   VARCHAR DEFAULT 'json',
                   is_compressed BOOLEAN DEFAULT FALSE,
                   created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                   PRIMARY KEY (asin, domain, raw_file_path)
               )"""
        )
        conn.execute(
            "ALTER TABLE curated.asin_raw_file_mapping "
            "ADD COLUMN IF NOT EXISTS file_format VARCHAR DEFAULT 'json'"
        )
        conn.execute(
            "ALTER TABLE curated.asin_raw_file_mapping "
            "ADD COLUMN IF NOT EXISTS is_compressed BOOLEAN DEFAULT FALSE"
        )

        conn.execute(
            """CREATE TABLE IF NOT EXISTS curated.discovery_expansion_state (
                   expansion_type       VARCHAR NOT NULL,
                   domain               INTEGER NOT NULL DEFAULT 1,
                   target_key           VARCHAR NOT NULL,
                   target_label         VARCHAR,
                   last_priority_score  DOUBLE,
                   last_candidate_count INTEGER DEFAULT 0,
                   last_new_asin_count  INTEGER DEFAULT 0,
                   total_new_asin_count INTEGER DEFAULT 0,
                   run_count            INTEGER DEFAULT 0,
                   first_run_at         TIMESTAMP,
                   last_run_at          TIMESTAMP,
                   notes                VARCHAR,
                   PRIMARY KEY (expansion_type, domain, target_key)
               )"""
        )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # ------------------------------------------------------------------
    # ASIN 注册表
    # ------------------------------------------------------------------

    def register_asins(
        self,
        asins: list[dict],
    ) -> int:
        """注册新发现的 ASIN, 已存在的不会覆盖.

        Parameters
        ----------
        asins : list[dict]
            每个 dict 至少包含 'asin', 可选 'domain', 'marketplace', 'product_title',
            'brand', 'category', 'discovery_source', 'search_term', 'priority'.

        Returns
        -------
        int
            新注册的 ASIN 数量.
        """
        now = _utc_now()

        # 先去重: 同一 (asin, domain) 只保留第一个
        seen: set[tuple[str, int]] = set()
        unique_items: list[dict] = []
        for item in asins:
            asin = item.get("asin", "").strip()
            domain = item.get("domain", 1)
            if not asin:
                continue
            key = (asin, domain)
            if key not in seen:
                seen.add(key)
                unique_items.append(item)

        if not unique_items:
            return 0

        # 查已有数量
        before_count = self.conn.execute(
            "SELECT COUNT(*) FROM curated.keepa_asin_registry"
        ).fetchone()[0]

        # 批量 INSERT ... ON CONFLICT DO NOTHING
        batch_size = 1000
        for i in range(0, len(unique_items), batch_size):
            batch = unique_items[i : i + batch_size]
            rows_data = []
            for item in batch:
                asin = item.get("asin", "").strip()
                domain = item.get("domain", 1)
                rows_data.append([
                    asin, domain,
                    item.get("marketplace"),
                    item.get("product_title"),
                    item.get("brand"),
                    item.get("category"),
                    item.get("category_id"),
                    item.get("discovery_source"),
                    item.get("search_term"),
                    item.get("priority", 0),
                    now,
                    item.get("notes"),
                ])

            placeholders = ", ".join(
                ["(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE, ?)"] * len(batch)
            )
            flat_params = [v for row in rows_data for v in row]

            self.conn.execute(
                f"""INSERT INTO curated.keepa_asin_registry
                   (asin, domain, marketplace, product_title, brand, category,
                    category_id, discovery_source, search_term, priority,
                    first_seen_at, is_active, notes)
                   VALUES {placeholders}
                   ON CONFLICT (asin, domain) DO NOTHING""",
                flat_params,
            )

        after_count = self.conn.execute(
            "SELECT COUNT(*) FROM curated.keepa_asin_registry"
        ).fetchone()[0]

        return after_count - before_count

    def get_asins_to_fetch(
        self,
        *,
        domain: int = 1,
        max_count: int = 100,
        stale_hours: int = 336,
    ) -> list[dict]:
        """获取需要更新的 ASIN 列表.

        优先级:
        1. 从未采集过的 ASIN (last_fetched_at IS NULL)
        2. 已分层 ASIN 按 business_tier 动态重采窗口判定是否过期
           - P0: 3-7 天
           - P1: 10-14 天
           - P2: 21-30 天
           - 其他分层 / 未分层: 使用 stale_hours 兜底
        3. 按 business_priority / priority DESC, first_seen_at ASC 排序

        Returns
        -------
        list[dict]
            包含 asin, domain, marketplace, last_fetched_at 等字段.
        """
        stale_hours_sql = _build_dynamic_stale_hours_sql(int(stale_hours))
        rows = self.conn.execute(
            f"""SELECT asin, domain, marketplace, product_title, brand,
                      category, category_id, priority, business_priority, business_tier, last_fetched_at
               FROM curated.keepa_asin_registry
               WHERE domain = ?
                 AND is_active = TRUE
                 AND (last_fetched_at IS NULL
                                            OR date_diff('hour', last_fetched_at, CURRENT_TIMESTAMP) >= ({stale_hours_sql}))
               ORDER BY
                 CASE WHEN last_fetched_at IS NULL THEN 0 ELSE 1 END,
                 COALESCE(business_priority, priority) DESC,
                 priority DESC,
                 first_seen_at ASC
               LIMIT ?""",
            [domain, max_count],
        ).fetchall()

        columns = [
            "asin", "domain", "marketplace", "product_title", "brand",
            "category", "category_id", "priority", "business_priority", "business_tier", "last_fetched_at",
        ]
        return [dict(zip(columns, row)) for row in rows]

    def mark_fetched(self, asin: str, domain: int = 1) -> None:
        """标记 ASIN 已采集."""
        now = _utc_now()
        self.conn.execute(
            """UPDATE curated.keepa_asin_registry
               SET last_fetched_at = ?, fetch_count = fetch_count + 1
               WHERE asin = ? AND domain = ?""",
            [now, asin, domain],
        )

    def update_asin_metadata(
        self,
        asin: str,
        domain: int,
        *,
        product_title: str | None = None,
        brand: str | None = None,
        category: str | None = None,
        category_path: str | None = None,
        root_category_id: int | None = None,
    ) -> None:
        """用 Keepa 返回的元数据更新注册表."""
        updates = []
        params: list[Any] = []
        if product_title:
            updates.append("product_title = ?")
            params.append(product_title)
        if brand:
            updates.append("brand = ?")
            params.append(brand)
        if category:
            updates.append("category = ?")
            params.append(category)
        if category_path:
            updates.append("category_path = ?")
            params.append(category_path)
        if root_category_id is not None:
            updates.append("root_category_id = ?")
            params.append(root_category_id)
        if not updates:
            return
        params.extend([asin, domain])
        self.conn.execute(
            f"UPDATE curated.keepa_asin_registry SET {', '.join(updates)} "
            f"WHERE asin = ? AND domain = ?",
            params,
        )

    def update_business_scores(self, rows: list[dict]) -> int:
        """批量回写业务评分结果到 ASIN 注册表."""
        if not rows:
            return 0

        score_updated_at = _utc_now()
        updated = 0
        for row in rows:
            asin = row.get("asin")
            domain = row.get("domain")
            if not asin or domain is None:
                continue

            self.conn.execute(
                """UPDATE curated.keepa_asin_registry
                   SET business_score_total = ?,
                       business_tier = ?,
                       business_priority = ?,
                       score_updated_at = ?
                   WHERE asin = ? AND domain = ?""",
                [
                    row.get("business_score_total"),
                    row.get("business_tier"),
                    row.get("business_priority"),
                    score_updated_at,
                    asin,
                    domain,
                ],
            )
            updated += 1
        return updated

    def get_business_tier_stats(self, domain: int = 1) -> dict[str, int]:
        """返回当前站点业务分层统计."""
        rows = self.conn.execute(
            """SELECT COALESCE(business_tier, 'UNSCORED') AS business_tier, COUNT(*)
               FROM curated.keepa_asin_registry
               WHERE domain = ? AND is_active = TRUE
               GROUP BY 1""",
            [domain],
        ).fetchall()
        return {str(tier): count for tier, count in rows}

    # ------------------------------------------------------------------
    # ASIN 自动停用 (is_active → FALSE)
    # ------------------------------------------------------------------

    def deactivate_asins(
        self,
        asin_domain_pairs: list[tuple[str, int]],
        reason: str,
    ) -> int:
        """将指定 ASIN 标记为 is_active=FALSE.

        Parameters
        ----------
        asin_domain_pairs : list[tuple[str, int]]
            [(asin, domain), ...]
        reason : str
            停用原因, 枚举参考:
            - 'no_data'          Keepa 查无此商品 (API 返回空)
            - 'all_prices_null'  所有价格字段近 90 天全为 NULL → 疑似下架
            - 'stale_90d'        连续 90 天无新数据点写入
            - 'fetch_failed_3x'  连续 3 次采集失败
            - 'manual'           人工停用

        Returns
        -------
        int
            实际停用的 ASIN 数量.
        """
        if not asin_domain_pairs:
            return 0

        now = _utc_now()
        count = 0
        for asin, domain in asin_domain_pairs:
            result = self.conn.execute(
                """UPDATE curated.keepa_asin_registry
                   SET is_active = FALSE, inactive_reason = ?, inactive_at = ?
                   WHERE asin = ? AND domain = ? AND is_active = TRUE""",
                [reason, now, asin, domain],
            )
            count += result.fetchone()[0] if hasattr(result, 'fetchone') else 1
        return count

    def reactivate_asins(
        self,
        asin_domain_pairs: list[tuple[str, int]],
    ) -> int:
        """重新激活之前停用的 ASIN."""
        if not asin_domain_pairs:
            return 0

        for asin, domain in asin_domain_pairs:
            self.conn.execute(
                """UPDATE curated.keepa_asin_registry
                   SET is_active = TRUE, inactive_reason = NULL, inactive_at = NULL
                   WHERE asin = ? AND domain = ?""",
                [asin, domain],
            )
        return len(asin_domain_pairs)

    # ------------------------------------------------------------------
    # 类目注册表
    # ------------------------------------------------------------------

    def sync_categories_from_csv(
        self,
        csv_path: str | Path,
        domain: int = 1,
        excluded_ids: set[int] | None = None,
        min_products: int = 50000,
    ) -> int:
        """从 CSV 同步类目到 keepa_category_registry (INSERT ... ON CONFLICT DO NOTHING).

        Returns
        -------
        int
            新插入的类目数量.
        """
        import csv as csv_mod

        csv_path = Path(csv_path)
        if not csv_path.exists():
            return 0

        excluded = excluded_ids or set()
        before = self.conn.execute(
            "SELECT COUNT(*) FROM curated.keepa_category_registry WHERE domain = ?",
            [domain],
        ).fetchone()[0]

        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv_mod.DictReader(f)
            order = 0
            for row in reader:
                row = {k.strip(): v.strip() if v else v for k, v in row.items()}
                cat_id = int(row["category_id"])
                count = int(row.get("product_count") or 0)

                if cat_id in excluded or count < min_products:
                    continue

                order += 1
                level = row.get("level", "L1")
                # 从 level 字段解析 depth: "L1" → 1, "L2" → 2, "L3" → 3
                depth = int(level[1]) if level and level.startswith("L") and len(level) >= 2 else 1
                self.conn.execute(
                    """INSERT INTO curated.keepa_category_registry
                       (category_id, domain, category_en, category_cn, parent_id,
                        level, depth, product_count, sort_order, is_active)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                       ON CONFLICT (category_id, domain) DO UPDATE SET
                         category_en = EXCLUDED.category_en,
                         category_cn = EXCLUDED.category_cn,
                         product_count = EXCLUDED.product_count,
                         depth = EXCLUDED.depth,
                         sort_order = EXCLUDED.sort_order""",
                    [
                        cat_id, domain,
                        row.get("category_en"),
                        row.get("category_cn"),
                        int(row["parent_id"]) if row.get("parent_id") else None,
                        level,
                        depth,
                        count,
                        order,
                    ],
                )

        after = self.conn.execute(
            "SELECT COUNT(*) FROM curated.keepa_category_registry WHERE domain = ?",
            [domain],
        ).fetchone()[0]
        return after - before

    def get_next_category_for_bestseller(self, domain: int = 1) -> dict | None:
        """获取下一个需要拉取 BestSeller 的类目.

        第一轮策略: 仅 L1 类目, product_count >= 50000.
        按 product_count DESC 排序, 从未拉取过的优先.
        全部拉完 → 返回 None.

        Returns
        -------
        dict or None
            {'category_id': ..., 'category_en': ..., 'category_cn': ..., 'product_count': ..., 'depth': ...}
        """
        row = self.conn.execute(
            """SELECT category_id, category_en, category_cn, product_count, depth
               FROM curated.keepa_category_registry
               WHERE domain = ? AND is_active = TRUE
                 AND bestseller_fetched_at IS NULL
                 AND depth = 1
                 AND product_count >= 50000
               ORDER BY product_count DESC
               LIMIT 1""",
            [domain],
        ).fetchone()
        if not row:
            return None
        return {
            "category_id": row[0],
            "category_en": row[1],
            "category_cn": row[2],
            "product_count": row[3],
            "depth": row[4],
        }

    def mark_category_bestseller_done(
        self, category_id: int, domain: int, asin_count: int
    ) -> None:
        """标记某类目的 BestSeller 已拉取."""
        self.conn.execute(
            """UPDATE curated.keepa_category_registry
               SET bestseller_fetched_at = ?, bestseller_asin_count = ?
               WHERE category_id = ? AND domain = ?""",
            [_utc_now(), asin_count, category_id, domain],
        )

    def get_category_stats(self, domain: int = 1) -> dict:
        """获取类目注册表统计.

        只统计符合 BestSeller 条件的类目 (depth=1, product_count >= 50000).
        """
        total = self.conn.execute(
            """SELECT COUNT(*) FROM curated.keepa_category_registry
               WHERE domain = ? AND is_active = TRUE AND depth = 1 AND product_count >= 50000""",
            [domain],
        ).fetchone()[0]
        fetched = self.conn.execute(
            """SELECT COUNT(*) FROM curated.keepa_category_registry
               WHERE domain = ? AND is_active = TRUE AND bestseller_fetched_at IS NOT NULL
                 AND depth = 1 AND product_count >= 50000""",
            [domain],
        ).fetchone()[0]
        total_all = self.conn.execute(
            "SELECT COUNT(*) FROM curated.keepa_category_registry WHERE domain = ? AND is_active = TRUE",
            [domain],
        ).fetchone()[0]
        return {
            "total_categories": total,
            "total_all_depths": total_all,
            "bestseller_fetched": fetched,
            "bestseller_pending": total - fetched,
        }

    def get_next_l1_for_children_fetch(self, domain: int = 1) -> dict | None:
        """获取下一个需要拉取子类目的 L1 类目.

        Returns
        -------
        dict or None
            {'category_id': ..., 'category_en': ..., 'category_cn': ..., 'product_count': ...}
        """
        row = self.conn.execute(
            """SELECT category_id, category_en, category_cn, product_count
               FROM curated.keepa_category_registry
               WHERE domain = ? AND is_active = TRUE
                 AND depth = 1
                 AND children_fetched_at IS NULL
               ORDER BY sort_order ASC
               LIMIT 1""",
            [domain],
        ).fetchone()
        if not row:
            return None
        return {
            "category_id": row[0],
            "category_en": row[1],
            "category_cn": row[2],
            "product_count": row[3],
        }

    def mark_category_children_fetched(
        self, category_id: int, domain: int
    ) -> None:
        """标记某 L1 类目的子类目已拉取."""
        self.conn.execute(
            """UPDATE curated.keepa_category_registry
               SET children_fetched_at = ?
               WHERE category_id = ? AND domain = ?""",
            [_utc_now(), category_id, domain],
        )

    def upsert_categories_from_tree(
        self,
        categories: list[dict],
        domain: int = 1,
    ) -> int:
        """从 categoryTree (product API) 或 category API 结果批量 upsert 类目.

        Parameters
        ----------
        categories : list[dict]
            每个 dict: category_id, category_en, parent_id, level, depth, product_count
        domain : int

        Returns
        -------
        int
            新插入的类目数量.
        """
        if not categories:
            return 0

        before = self.conn.execute(
            "SELECT COUNT(*) FROM curated.keepa_category_registry WHERE domain = ?",
            [domain],
        ).fetchone()[0]

        for cat in categories:
            self.conn.execute(
                """INSERT INTO curated.keepa_category_registry
                   (category_id, domain, category_en, category_cn, parent_id,
                    level, depth, product_count, is_active)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                   ON CONFLICT (category_id, domain) DO UPDATE SET
                     category_en = COALESCE(EXCLUDED.category_en, curated.keepa_category_registry.category_en),
                     product_count = CASE
                       WHEN EXCLUDED.product_count > 0 THEN EXCLUDED.product_count
                       ELSE curated.keepa_category_registry.product_count END,
                     depth = COALESCE(curated.keepa_category_registry.depth, EXCLUDED.depth)""",
                [
                    cat["category_id"], domain,
                    cat.get("category_en"),
                    cat.get("category_cn"),
                    cat.get("parent_id"),
                    cat.get("level", f"L{cat.get('depth', 1)}"),
                    cat.get("depth", 1),
                    cat.get("product_count", 0),
                ],
            )

        after = self.conn.execute(
            "SELECT COUNT(*) FROM curated.keepa_category_registry WHERE domain = ?",
            [domain],
        ).fetchone()[0]
        return after - before

    def find_asins_to_deactivate(self, domain: int = 1) -> dict[str, list[tuple[str, int]]]:
        """扫描注册表, 返回建议停用的 ASIN 及原因.

        检测规则:
        1. no_data:          已采集但历史表中无任何数据行
        2. all_prices_null:  近 90 天所有价格字段 (amazon/new/buybox) 全为 NULL
        3. stale_90d:        last_fetched_at 超过 90 天未更新 (多站点轮转周期过长、采集反复跳过)
        4. fetch_failed_3x:  fetch_count >= 3 但历史表行数为 0

        Returns
        -------
        dict[str, list[tuple[str, int]]]
            {reason: [(asin, domain), ...]}
        """
        result: dict[str, list[tuple[str, int]]] = {
            "no_data": [],
            "all_prices_null": [],
            "stale_90d": [],
            "fetch_failed_3x": [],
        }

        # Rule 1: 已采集 (fetch_count > 0) 但历史表中无数据
        rows = self.conn.execute(
            """SELECT r.asin, r.domain
               FROM curated.keepa_asin_registry r
               LEFT JOIN (
                   SELECT asin, domain, COUNT(*) as row_count
                   FROM curated.keepa_product_history
                   GROUP BY asin, domain
               ) h ON r.asin = h.asin AND r.domain = h.domain
               WHERE r.domain = ? AND r.is_active = TRUE
                 AND r.fetch_count > 0
                 AND (h.row_count IS NULL OR h.row_count = 0)""",
            [domain],
        ).fetchall()
        result["no_data"] = [(r[0], r[1]) for r in rows]

        # Rule 2: 近 90 天所有价格均为 NULL (商品已下架)
        rows = self.conn.execute(
            """SELECT r.asin, r.domain
               FROM curated.keepa_asin_registry r
               WHERE r.domain = ? AND r.is_active = TRUE AND r.fetch_count > 0
                 AND NOT EXISTS (
                     SELECT 1 FROM curated.keepa_product_history h
                     WHERE h.asin = r.asin AND h.domain = r.domain
                       AND h.date >= CURRENT_DATE - 90
                       AND (h.amazon_price IS NOT NULL
                            OR h.new_price IS NOT NULL
                            OR h.buy_box_price IS NOT NULL)
                 )
                 AND EXISTS (
                     SELECT 1 FROM curated.keepa_product_history h2
                     WHERE h2.asin = r.asin AND h2.domain = r.domain
                 )""",
            [domain],
        ).fetchall()
        # 排除已被 Rule 1 标记的
        no_data_set = set(result["no_data"])
        result["all_prices_null"] = [
            (r[0], r[1]) for r in rows if (r[0], r[1]) not in no_data_set
        ]

        # Rule 3: 超过 90 天未被采集更新
        rows = self.conn.execute(
            """SELECT asin, domain
               FROM curated.keepa_asin_registry
               WHERE domain = ? AND is_active = TRUE
                 AND last_fetched_at IS NOT NULL
                 AND last_fetched_at < CURRENT_TIMESTAMP - INTERVAL '90 days'""",
            [domain],
        ).fetchall()
        result["stale_90d"] = [(r[0], r[1]) for r in rows]

        # Rule 4: 采集 ≥3 次但历史表为空 (反复失败)
        rows = self.conn.execute(
            """SELECT r.asin, r.domain
               FROM curated.keepa_asin_registry r
               LEFT JOIN (
                   SELECT asin, domain, COUNT(*) as row_count
                   FROM curated.keepa_product_history
                   GROUP BY asin, domain
               ) h ON r.asin = h.asin AND r.domain = h.domain
               WHERE r.domain = ? AND r.is_active = TRUE
                 AND r.fetch_count >= 3
                 AND (h.row_count IS NULL OR h.row_count = 0)""",
            [domain],
        ).fetchall()
        result["fetch_failed_3x"] = [(r[0], r[1]) for r in rows]

        return result

    def run_auto_deactivation(self, domain: int = 1, dry_run: bool = False) -> dict:
        """执行自动停用扫描并应用.

        Parameters
        ----------
        domain : int
            站点 ID.
        dry_run : bool
            True=只检测不执行, False=检测并停用.

        Returns
        -------
        dict
            {reason: count} 统计.
        """
        candidates = self.find_asins_to_deactivate(domain)
        stats: dict[str, int] = {}

        for reason, pairs in candidates.items():
            if not pairs:
                stats[reason] = 0
                continue
            if dry_run:
                stats[reason] = len(pairs)
            else:
                self.deactivate_asins(pairs, reason)
                stats[reason] = len(pairs)

        return stats

    # ------------------------------------------------------------------
    # Keepa 历史数据入库
    # ------------------------------------------------------------------

    def ingest_keepa_product_snapshots(
        self,
        rows: list[dict],
        domain: int = 1,
    ) -> int:
        """将 Keepa 商品快照写入 DuckDB, 每个 ASIN 保留最新一次."""
        count = 0
        for row in rows:
            asin = row.get("asin")
            if not asin:
                continue

            ingested_at = _utc_now()

            self.conn.execute(
                """INSERT INTO curated.keepa_product_snapshot
                   (asin, domain, marketplace, product_title, brand, category,
                    price, list_price, bsr, rating, review_count, estimated_sales,
                    estimated_sales_period, seller_count, total_offer_count,
                    offer_count_fba, offer_count_fbm, retrieved_offer_count,
                    offers_successful, stock_status, data_capture_time,
                    source_url, keepa_last_update, ingested_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (asin, domain) DO UPDATE SET
                     marketplace = EXCLUDED.marketplace,
                     product_title = EXCLUDED.product_title,
                     brand = EXCLUDED.brand,
                     category = EXCLUDED.category,
                     price = EXCLUDED.price,
                     list_price = EXCLUDED.list_price,
                     bsr = EXCLUDED.bsr,
                     rating = EXCLUDED.rating,
                     review_count = EXCLUDED.review_count,
                     estimated_sales = EXCLUDED.estimated_sales,
                     estimated_sales_period = EXCLUDED.estimated_sales_period,
                     seller_count = EXCLUDED.seller_count,
                     total_offer_count = EXCLUDED.total_offer_count,
                     offer_count_fba = EXCLUDED.offer_count_fba,
                     offer_count_fbm = EXCLUDED.offer_count_fbm,
                     retrieved_offer_count = EXCLUDED.retrieved_offer_count,
                     offers_successful = EXCLUDED.offers_successful,
                     stock_status = EXCLUDED.stock_status,
                     data_capture_time = EXCLUDED.data_capture_time,
                     source_url = EXCLUDED.source_url,
                     keepa_last_update = EXCLUDED.keepa_last_update,
                     ingested_at = EXCLUDED.ingested_at""",
                [
                    asin,
                    domain,
                    row.get("marketplace"),
                    row.get("product_title"),
                    row.get("brand"),
                    row.get("category"),
                    _safe_float(row.get("price")),
                    _safe_float(row.get("list_price")),
                    _safe_int(row.get("bsr")),
                    _safe_float(row.get("rating")),
                    _safe_int(row.get("review_count")),
                    _safe_int(row.get("estimated_sales")),
                    row.get("estimated_sales_period"),
                    _safe_int(row.get("seller_count")),
                    _safe_int(row.get("total_offer_count")),
                    _safe_int(row.get("offer_count_fba")),
                    _safe_int(row.get("offer_count_fbm")),
                    _safe_int(row.get("retrieved_offer_count")),
                    row.get("offers_successful"),
                    row.get("stock_status"),
                    row.get("data_capture_time"),
                    row.get("source_url"),
                    _safe_int(row.get("keepa_last_update")),
                    ingested_at,
                ],
            )

            self.conn.execute(
                """UPDATE curated.keepa_asin_registry
                   SET last_snapshot_at = COALESCE(?, CURRENT_TIMESTAMP)
                   WHERE asin = ? AND domain = ?""",
                [row.get("data_capture_time"), asin, domain],
            )
            count += 1

        return count

    def ingest_keepa_history(
        self,
        rows: list[dict],
        domain: int = 1,
    ) -> int:
        """将 Keepa 历史数据写入 DuckDB.

        使用 DELETE + 批量 INSERT 实现幂等: 先删除本批 ASIN 的旧数据,
        再批量 INSERT 新数据。整个过程包裹在事务中, 避免 delete 成功后 insert 异常
        导致该批 ASIN 历史被清空。

        Returns
        -------
        int
            写入行数.
        """
        if not rows:
            return 0

        # 构建参数列表, 同时收集涉及的 ASIN 用于批量 DELETE
        params = []
        batch_asins: set[str] = set()
        for row in rows:
            asin = row.get("asin")
            date_str = row.get("date")
            if not asin or not date_str:
                continue
            batch_asins.add(asin)
            params.append((
                asin, domain, str(date_str),
                _safe_float(row.get("amazon_price")),
                _safe_float(row.get("new_price")),
                _safe_float(row.get("used_price")),
                _safe_float(row.get("buy_box_price")),
                _safe_float(row.get("list_price")),
                _safe_int(row.get("bsr")),
                _safe_float(row.get("rating")),
                _safe_int(row.get("review_count")),
                _safe_int(row.get("monthly_sold")),
                _safe_int(row.get("new_offer_count")),
                _safe_int(row.get("used_offer_count")),
            ))

        if not params:
            return 0

        # 先删除这批 ASIN 在该 domain 下的旧历史, 避免主键冲突
        asin_list = list(batch_asins)
        placeholders = ", ".join(["?"] * len(asin_list))
        self.conn.execute("BEGIN TRANSACTION")
        try:
            self.conn.execute(
                f"DELETE FROM curated.keepa_product_history "
                f"WHERE domain = ? AND asin IN ({placeholders})",
                [domain] + asin_list,
            )
            self.conn.executemany(
                """INSERT INTO curated.keepa_product_history
                   (asin, domain, date, amazon_price, new_price, used_price,
                    buy_box_price, list_price, bsr, rating, review_count,
                    monthly_sold, new_offer_count, used_offer_count, ingested_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                params,
            )
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise
        return len(params)

    def get_latest_history_date(self, asin: str, domain: int = 1) -> str | None:
        """获取某 ASIN 在 DuckDB 中的最新历史日期."""
        result = self.conn.execute(
            "SELECT MAX(date) FROM curated.keepa_product_history WHERE asin = ? AND domain = ?",
            [asin, domain],
        ).fetchone()
        if result and result[0]:
            return str(result[0])
        return None

    # ------------------------------------------------------------------
    # ASIN ↔ 关键词映射
    # ------------------------------------------------------------------

    def upsert_asin_keywords(
        self,
        asin: str,
        domain: int,
        keywords: list[str],
        source: str = "title_extract",
    ) -> int:
        """保存 ASIN 的关键词映射 (已存在的跳过).

        Returns
        -------
        int
            新增映射数量.
        """
        geo = _DOMAIN_TO_GEO.get(domain, "US")
        count = 0
        for kw in keywords:
            kw = kw.strip()
            if not kw:
                continue
            try:
                self.conn.execute(
                    """INSERT INTO curated.asin_keyword_mapping
                       (asin, domain, keyword, geo, source, created_at)
                       VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                       ON CONFLICT (asin, domain, keyword) DO UPDATE
                       SET geo = EXCLUDED.geo""",
                    [asin, domain, kw, geo, source],
                )
                count += 1
            except Exception:
                pass
        return count

    def upsert_asin_keywords_batch(
        self,
        mappings: list[tuple[str, int, list[str]]],
        source: str = "title_extract",
    ) -> int:
        """批量保存 ASIN 关键词映射.

        Parameters
        ----------
        mappings : list of (asin, domain, keywords_list)

        Returns
        -------
        int
            新增映射数量.
        """
        count = 0
        for asin, domain, keywords in mappings:
            geo = _DOMAIN_TO_GEO.get(domain, "US")
            for kw in keywords:
                kw = kw.strip()
                if not kw:
                    continue
                try:
                    self.conn.execute(
                        """INSERT INTO curated.asin_keyword_mapping
                           (asin, domain, keyword, geo, source, created_at)
                           VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                           ON CONFLICT (asin, domain, keyword) DO UPDATE
                           SET geo = EXCLUDED.geo""",
                        [asin, domain, kw, geo, source],
                    )
                    count += 1
                except Exception:
                    pass
        return count

    def get_keywords_for_asin(
        self,
        asin: str,
        domain: int = 1,
    ) -> list[str]:
        """获取 ASIN 的关键词列表."""
        rows = self.conn.execute(
            """SELECT keyword FROM curated.asin_keyword_mapping
               WHERE asin = ? AND domain = ?
               ORDER BY created_at""",
            [asin, domain],
        ).fetchall()
        return [r[0] for r in rows]

    def get_asins_for_keyword(
        self,
        keyword: str,
        domain: int = 1,
    ) -> list[str]:
        """获取使用某关键词的 ASIN 列表."""
        rows = self.conn.execute(
            """SELECT asin FROM curated.asin_keyword_mapping
               WHERE keyword = ? AND domain = ?""",
            [keyword, domain],
        ).fetchall()
        return [r[0] for r in rows]

    def upsert_asin_raw_file_mappings(
        self,
        *,
        asins: list[str],
        domain: int,
        source: str,
        raw_file_path: str | Path,
    ) -> int:
        """批量写入 ASIN ↔ 原始文件路径映射."""
        unique_asins = sorted({asin.strip() for asin in asins if asin and asin.strip()})
        if not unique_asins:
            return 0

        raw_path = Path(raw_file_path)
        created_at = _utc_now()
        suffixes = raw_path.suffixes
        is_compressed = suffixes[-1:] == [".gz"]
        if suffixes[-2:] == [".json", ".gz"]:
            file_format = "json.gz"
        elif suffixes[-1:] == [".json"]:
            file_format = "json"
        else:
            file_format = raw_path.suffix.lstrip(".") or "unknown"

        for asin in unique_asins:
            self.conn.execute(
                """INSERT INTO curated.asin_raw_file_mapping
                   (asin, domain, source, raw_file_path, file_format, is_compressed, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (asin, domain, raw_file_path) DO UPDATE
                   SET source = EXCLUDED.source,
                       file_format = EXCLUDED.file_format,
                       is_compressed = EXCLUDED.is_compressed,
                       created_at = EXCLUDED.created_at""",
                [
                    asin,
                    domain,
                    source,
                    str(raw_path),
                    file_format,
                    is_compressed,
                    created_at,
                ],
            )
        return len(unique_asins)

    def record_discovery_expansion(
        self,
        *,
        expansion_type: str,
        domain: int,
        target_key: str,
        target_label: str | None = None,
        priority_score: float | None = None,
        candidate_count: int = 0,
        new_asin_count: int = 0,
        notes: str | None = None,
    ) -> None:
        """记录 keyword / subcategory 扩张动作的最近执行状态."""
        now = _utc_now()
        self.conn.execute(
            """INSERT INTO curated.discovery_expansion_state
               (expansion_type, domain, target_key, target_label,
                last_priority_score, last_candidate_count, last_new_asin_count,
                total_new_asin_count, run_count, first_run_at, last_run_at, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT (expansion_type, domain, target_key) DO UPDATE SET
                 target_label = COALESCE(EXCLUDED.target_label, curated.discovery_expansion_state.target_label),
                 last_priority_score = EXCLUDED.last_priority_score,
                 last_candidate_count = EXCLUDED.last_candidate_count,
                 last_new_asin_count = EXCLUDED.last_new_asin_count,
                 total_new_asin_count = COALESCE(curated.discovery_expansion_state.total_new_asin_count, 0) + EXCLUDED.last_new_asin_count,
                 run_count = COALESCE(curated.discovery_expansion_state.run_count, 0) + 1,
                 last_run_at = EXCLUDED.last_run_at,
                 notes = EXCLUDED.notes""",
            [
                expansion_type,
                domain,
                target_key,
                target_label,
                priority_score,
                candidate_count,
                new_asin_count,
                new_asin_count,
                1,
                now,
                now,
                notes,
            ],
        )

    def get_keyword_expansion_candidates(
        self,
        *,
        domain: int = 1,
        limit: int = 5,
        cooldown_hours: int = 72,
        min_mapped_asins: int = 3,
        max_mapped_asins: int = 50,
    ) -> list[dict[str, Any]]:
        """返回下一阶段 keyword 扩张候选队列."""
        rows = self.conn.execute(
            f"""
            WITH mapped AS (
                SELECT
                    m.domain,
                    m.geo,
                    m.keyword,
                    COUNT(DISTINCT m.asin) AS mapped_asin_count,
                    COUNT(DISTINCT CASE WHEN COALESCE(r.business_priority, r.priority, 0) >= 90 THEN m.asin END) AS mapped_p0_asin_count,
                    COUNT(DISTINCT NULLIF(split_part(r.category_path, ' > ', 2), '')) AS l2_category_count,
                    COUNT(DISTINCT NULLIF(split_part(r.category_path, ' > ', 3), '')) AS l3_category_count,
                    MAX(COALESCE(r.business_priority, r.priority, 0)) AS max_business_priority
                FROM curated.asin_keyword_mapping m
                JOIN curated.keepa_asin_registry r
                  ON r.asin = m.asin AND r.domain = m.domain
                WHERE r.is_active = TRUE
                  AND m.domain = ?
                GROUP BY 1, 2, 3
            ),
            trend AS (
                SELECT
                    keyword,
                    geo,
                    AVG(trend_index) FILTER (
                        WHERE date >= CURRENT_DATE - INTERVAL 30 DAY
                    ) AS trend_30d_avg,
                    AVG(trend_index) FILTER (
                        WHERE date >= CURRENT_DATE - INTERVAL 7 DAY
                    ) AS last_7d_avg,
                    AVG(trend_index) FILTER (
                        WHERE date >= CURRENT_DATE - INTERVAL 14 DAY
                          AND date < CURRENT_DATE - INTERVAL 7 DAY
                    ) AS prev_7d_avg
                FROM curated.google_trends_daily t
                WHERE date >= CURRENT_DATE - INTERVAL 30 DAY
                GROUP BY 1, 2
            ),
            trend_hot AS (
                SELECT
                    t.keyword,
                    t.geo,
                    COUNT(*) FILTER (
                        WHERE t.date >= CURRENT_DATE - INTERVAL 7 DAY
                          AND t.trend_index > tr.trend_30d_avg
                    ) AS hot_days_over_30d_avg
                FROM curated.google_trends_daily t
                JOIN trend tr ON tr.keyword = t.keyword AND tr.geo = t.geo
                WHERE t.date >= CURRENT_DATE - INTERVAL 30 DAY
                GROUP BY 1, 2
            )
            SELECT
                m.domain,
                m.geo,
                m.keyword,
                m.mapped_asin_count,
                m.mapped_p0_asin_count,
                m.l2_category_count,
                m.l3_category_count,
                m.max_business_priority,
                tr.trend_30d_avg,
                tr.last_7d_avg,
                tr.prev_7d_avg,
                th.hot_days_over_30d_avg,
                s.last_run_at
            FROM mapped m
            LEFT JOIN trend tr
              ON tr.keyword = m.keyword AND tr.geo = m.geo
            LEFT JOIN trend_hot th
              ON th.keyword = m.keyword AND th.geo = m.geo
            LEFT JOIN curated.discovery_expansion_state s
              ON s.expansion_type = 'keyword'
             AND s.domain = m.domain
             AND s.target_key = m.keyword
            WHERE m.mapped_asin_count BETWEEN ? AND ?
            """,
            [domain, min_mapped_asins, max_mapped_asins],
        ).fetchall()

        columns = [
            "domain", "geo", "keyword", "mapped_asin_count", "mapped_p0_asin_count",
            "l2_category_count", "l3_category_count", "max_business_priority",
            "trend_30d_avg", "last_7d_avg", "prev_7d_avg", "hot_days_over_30d_avg", "last_run_at",
        ]
        candidates: list[dict[str, Any]] = []
        for row in rows:
            item = dict(zip(columns, row))
            last_run_at = item.get("last_run_at")
            if last_run_at is not None:
                hours_since = (datetime.now(timezone.utc).replace(tzinfo=None) - last_run_at).total_seconds() / 3600
                if hours_since < cooldown_hours:
                    continue

            trend_30d_avg = float(item.get("trend_30d_avg") or 0)
            last_7d_avg = float(item.get("last_7d_avg") or 0)
            prev_7d_avg = float(item.get("prev_7d_avg") or 0)
            trend_growth_7d = (last_7d_avg / prev_7d_avg) if prev_7d_avg > 0 else None
            hot_days = int(item.get("hot_days_over_30d_avg") or 0)
            mapped_asins = int(item.get("mapped_asin_count") or 0)
            mapped_p0 = int(item.get("mapped_p0_asin_count") or 0)
            max_priority = int(item.get("max_business_priority") or 0)
            l2_count = int(item.get("l2_category_count") or 0)
            l3_count = int(item.get("l3_category_count") or 0)

            # --- hard filters ---
            # 1. 基础热度门槛
            if trend_30d_avg < 10:
                continue
            # 2. 高热度 (≥25) 可直接入选; 中等热度需有增长趋势 (≥1.05)
            is_high_heat = trend_30d_avg >= 25
            is_growing = trend_growth_7d is not None and trend_growth_7d >= 1.05
            if not is_high_heat and not is_growing:
                continue
            # 3. 关键词最短长度
            if len(str(item.get("keyword") or "").strip()) < 4:
                continue

            # --- scoring: 趋势信号权重占主导 ---
            # 绝对热度 (0-4)
            if trend_30d_avg >= 50:
                trend_level_score = 4
            elif trend_30d_avg >= 30:
                trend_level_score = 3
            elif trend_30d_avg >= 20:
                trend_level_score = 2
            else:
                trend_level_score = 1

            # 增长势头 (0-4)
            if trend_growth_7d is not None and trend_growth_7d >= 1.30:
                trend_growth_score = 4
            elif trend_growth_7d is not None and trend_growth_7d >= 1.15:
                trend_growth_score = 3
            elif trend_growth_7d is not None and trend_growth_7d >= 1.05:
                trend_growth_score = 2
            elif trend_growth_7d is not None and trend_growth_7d >= 1.0:
                trend_growth_score = 1
            else:
                trend_growth_score = 0

            # hot_days 加分 (0-2), 不再作为硬门槛
            hot_days_score = 2 if hot_days >= 4 else 1 if hot_days >= 2 else 0

            # 覆盖度 (1-2)
            coverage_gap_score = 2 if 3 <= mapped_asins <= 20 else 1
            # 质量锚点 (0-2)
            quality_anchor_score = 2 if mapped_p0 >= 1 else 1 if max_priority >= 70 else 0
            # 类目匹配 (0-1)
            category_match_score = 1 if (l2_count >= 1 or l3_count >= 1) else 0

            # 总分: 趋势 (0-10) + 商品侧 (0-5) → 趋势信号占 2/3 权重
            expand_priority = (
                trend_level_score
                + trend_growth_score
                + hot_days_score
                + coverage_gap_score
                + quality_anchor_score
                + category_match_score
            )

            item.update(
                {
                    "trend_growth_7d": trend_growth_7d,
                    "trend_level_score": trend_level_score,
                    "trend_growth_score": trend_growth_score,
                    "hot_days_score": hot_days_score,
                    "coverage_gap_score": coverage_gap_score,
                    "quality_anchor_score": quality_anchor_score,
                    "category_match_score": category_match_score,
                    "expand_priority": expand_priority,
                }
            )
            candidates.append(item)

        # 排序: 总分 → 绝对热度 → 增长率 → P0 锚点 → 覆盖缺口
        candidates.sort(
            key=lambda item: (
                item["expand_priority"],
                item.get("trend_30d_avg") or 0,
                item.get("trend_growth_7d") or 0,
                item.get("mapped_p0_asin_count") or 0,
                -(item.get("mapped_asin_count") or 0),
            ),
            reverse=True,
        )
        return candidates[:limit]

    def get_subcategory_expansion_candidates(
        self,
        *,
        domain: int = 1,
        limit: int = 2,
        cooldown_hours: int = 720,
        min_sample_asins: int = 10,
    ) -> list[dict[str, Any]]:
        """返回下一阶段 L2/L3/L4 shortlist 扩张候选类目."""
        rows = self.conn.execute(
            """
            WITH latest_history AS (
                SELECT *
                FROM (
                    SELECT
                        asin,
                        domain,
                        COALESCE(buy_box_price, amazon_price, new_price, list_price) AS latest_effective_price,
                        monthly_sold AS latest_monthly_sold,
                        new_offer_count AS latest_new_offer_count,
                        ROW_NUMBER() OVER (PARTITION BY asin, domain ORDER BY date DESC) AS rn
                    FROM curated.keepa_product_history
                    WHERE domain = ?
                      AND date >= CURRENT_DATE - INTERVAL 30 DAY
                )
                WHERE rn = 1
            ),
            history_30d AS (
                SELECT
                    asin,
                    domain,
                    AVG(new_offer_count) AS avg_new_offer_count_30d,
                    COUNT(*) AS history_days_30d
                FROM curated.keepa_product_history
                WHERE domain = ?
                  AND date >= CURRENT_DATE - INTERVAL 30 DAY
                GROUP BY 1, 2
            ),
            trend_by_keyword AS (
                SELECT
                    m.asin,
                    m.domain,
                    AVG(t.trend_index) FILTER (
                        WHERE t.date >= CURRENT_DATE - INTERVAL 30 DAY
                    ) AS avg_trend_30d,
                    AVG(t.trend_index) FILTER (
                        WHERE t.date >= CURRENT_DATE - INTERVAL 7 DAY
                    ) AS last_7d_avg,
                    AVG(t.trend_index) FILTER (
                        WHERE t.date >= CURRENT_DATE - INTERVAL 14 DAY
                          AND t.date < CURRENT_DATE - INTERVAL 7 DAY
                    ) AS prev_7d_avg
                FROM curated.asin_keyword_mapping m
                LEFT JOIN curated.google_trends_daily t
                  ON t.keyword = m.keyword
                 AND t.geo = m.geo
                 AND t.date >= CURRENT_DATE - INTERVAL 30 DAY
                WHERE m.domain = ?
                GROUP BY 1, 2
            ),
            snapshot_now AS (
                SELECT
                    asin,
                    domain,
                    COALESCE(total_offer_count, seller_count, retrieved_offer_count) AS current_offer_count
                FROM curated.keepa_product_snapshot
                WHERE domain = ?
            ),
            path_parts AS (
                SELECT
                    r.asin,
                    r.domain,
                    r.root_category_id,
                    r.category_path,
                    NULLIF(TRIM(split_part(r.category_path, ' > ', 2)), '') AS l2_name,
                    NULLIF(TRIM(split_part(r.category_path, ' > ', 3)), '') AS l3_name,
                    NULLIF(TRIM(split_part(r.category_path, ' > ', 4)), '') AS l4_name
                FROM curated.keepa_asin_registry r
                WHERE r.is_active = TRUE
                  AND r.domain = ?
                  AND r.root_category_id IS NOT NULL
                  AND r.category_path IS NOT NULL
                  AND r.category_path <> ''
            ),
            categorized AS (
                SELECT
                    p.asin,
                    p.domain,
                    COALESCE(kr4.category_id, kr3.category_id, kr2.category_id) AS target_category_id,
                    COALESCE(
                        COALESCE(kr4.category_cn, kr4.category_en),
                        COALESCE(kr3.category_cn, kr3.category_en),
                        COALESCE(kr2.category_cn, kr2.category_en)
                    ) AS target_category_name,
                    CASE
                        WHEN kr4.category_id IS NOT NULL THEN concat_ws(' > ', split_part(p.category_path, ' > ', 1), p.l2_name, p.l3_name, p.l4_name)
                        WHEN kr3.category_id IS NOT NULL THEN concat_ws(' > ', split_part(p.category_path, ' > ', 1), p.l2_name, p.l3_name)
                        WHEN kr2.category_id IS NOT NULL THEN concat_ws(' > ', split_part(p.category_path, ' > ', 1), p.l2_name)
                        ELSE NULL
                    END AS target_category_path,
                    CASE
                        WHEN kr4.category_id IS NOT NULL THEN 4
                        WHEN kr3.category_id IS NOT NULL THEN 3
                        WHEN kr2.category_id IS NOT NULL THEN 2
                        ELSE NULL
                    END AS target_category_depth
                FROM path_parts p
                LEFT JOIN curated.keepa_category_registry kr2
                  ON kr2.domain = p.domain
                 AND kr2.parent_id = p.root_category_id
                 AND kr2.depth = 2
                 AND (kr2.category_en = p.l2_name OR kr2.category_cn = p.l2_name)
                LEFT JOIN curated.keepa_category_registry kr3
                  ON kr3.domain = p.domain
                 AND kr3.parent_id = kr2.category_id
                 AND kr3.depth = 3
                 AND (kr3.category_en = p.l3_name OR kr3.category_cn = p.l3_name)
                LEFT JOIN curated.keepa_category_registry kr4
                  ON kr4.domain = p.domain
                 AND kr4.parent_id = kr3.category_id
                 AND kr4.depth = 4
                 AND (kr4.category_en = p.l4_name OR kr4.category_cn = p.l4_name)
            )
            SELECT
                c.domain,
                c.target_category_id AS category_id,
                c.target_category_name AS category_name,
                c.target_category_path AS category_path,
                c.target_category_depth AS category_depth,
                MAX(COALESCE(kt.product_count, 0)) AS category_product_count,
                COUNT(*) AS sample_asin_count,
                MEDIAN(lh.latest_effective_price) AS median_effective_price_30d,
                AVG(lh.latest_monthly_sold) AS avg_monthly_sold_30d,
                AVG(COALESCE(sn.current_offer_count, h30.avg_new_offer_count_30d, lh.latest_new_offer_count)) AS avg_offer_count_30d,
                AVG(tb.avg_trend_30d) AS trend_index_30d,
                MAX(CASE WHEN tb.prev_7d_avg IS NOT NULL AND tb.prev_7d_avg > 0 THEN tb.last_7d_avg / tb.prev_7d_avg ELSE NULL END) AS trend_growth_7d,
                AVG(h30.history_days_30d) AS avg_history_days_30d,
                MAX(s.last_run_at) AS last_run_at
            FROM categorized c
            LEFT JOIN latest_history lh
              ON lh.asin = c.asin AND lh.domain = c.domain
            LEFT JOIN history_30d h30
              ON h30.asin = c.asin AND h30.domain = c.domain
            LEFT JOIN trend_by_keyword tb
              ON tb.asin = c.asin AND tb.domain = c.domain
            LEFT JOIN snapshot_now sn
              ON sn.asin = c.asin AND sn.domain = c.domain
            LEFT JOIN curated.keepa_category_registry kt
              ON kt.category_id = c.target_category_id AND kt.domain = c.domain
            LEFT JOIN curated.discovery_expansion_state s
              ON s.expansion_type = 'category'
             AND s.domain = c.domain
             AND s.target_key = CAST(c.target_category_id AS VARCHAR)
            WHERE c.target_category_id IS NOT NULL
            GROUP BY 1, 2, 3, 4, 5
            """,
            [domain, domain, domain, domain, domain],
        ).fetchall()

        columns = [
            "domain", "category_id", "category_name", "category_path", "category_depth", "category_product_count",
            "sample_asin_count", "median_effective_price_30d", "avg_monthly_sold_30d",
            "avg_offer_count_30d", "trend_index_30d", "trend_growth_7d", "avg_history_days_30d", "last_run_at",
        ]
        min_price, max_price = _DOMAIN_PRICE_BANDS.get(domain, (15.0, 60.0))
        candidates: list[dict[str, Any]] = []
        for row in rows:
            item = dict(zip(columns, row))
            if int(item.get("category_depth") or 0) not in (2, 3, 4):
                continue
            if int(item.get("sample_asin_count") or 0) < min_sample_asins:
                continue

            last_run_at = item.get("last_run_at")
            if last_run_at is not None:
                hours_since = (datetime.now(timezone.utc).replace(tzinfo=None) - last_run_at).total_seconds() / 3600
                if hours_since < cooldown_hours:
                    continue

            median_price = float(item.get("median_effective_price_30d") or 0)
            avg_monthly_sold = float(item.get("avg_monthly_sold_30d") or 0)
            avg_offer_count = float(item.get("avg_offer_count_30d") or 999)
            trend_index = float(item.get("trend_index_30d") or 0)
            trend_growth = float(item.get("trend_growth_7d") or 0)
            sample_asins = int(item.get("sample_asin_count") or 0)
            product_count = int(item.get("category_product_count") or 0)

            demand_score = 2 if avg_monthly_sold >= 200 else 1 if avg_monthly_sold >= 50 else 0
            trend_score = 2 if trend_index >= 20 and trend_growth >= 1.15 else 1 if trend_index >= 10 else 0
            competition_score = 2 if avg_offer_count <= 8 else 1 if avg_offer_count <= 12 else 0
            if min_price <= median_price <= max_price:
                price_score = 2
            elif min_price * 0.75 <= median_price <= max_price * 1.25:
                price_score = 1
            else:
                price_score = 0
            coverage_gap_score = 2 if product_count >= max(sample_asins * 80, 5000) else 1 if product_count >= max(sample_asins * 30, 1000) else 0

            shortlist_score = demand_score + trend_score + competition_score + price_score + coverage_gap_score
            item.update(
                {
                    "demand_score": demand_score,
                    "trend_score": trend_score,
                    "competition_score": competition_score,
                    "price_score": price_score,
                    "coverage_gap_score": coverage_gap_score,
                    "shortlist_score": shortlist_score,
                }
            )
            if shortlist_score < 3:
                continue

            candidates.append(item)

        candidates.sort(
            key=lambda item: (
                item["shortlist_score"],
                item.get("category_depth") or 0,
                item.get("trend_score") or 0,
                item.get("demand_score") or 0,
                item.get("coverage_gap_score") or 0,
                item.get("sample_asin_count") or 0,
            ),
            reverse=True,
        )
        return candidates[:limit]

    # ------------------------------------------------------------------
    # Google Trends 数据入库
    # ------------------------------------------------------------------

    def ingest_google_trends(
        self,
        rows: list[dict],
    ) -> int:
        """将 Google Trends 数据写入 DuckDB."""
        count = 0
        for row in rows:
            keyword = row.get("keyword_or_domain") or row.get("keyword")
            date_str = row.get("date")
            if not keyword or not date_str:
                continue

            geo = row.get("country") or row.get("geo") or "GLOBAL"
            self.conn.execute(
                """INSERT OR REPLACE INTO curated.google_trends_daily
                   (keyword, geo, date, trend_index, search_volume, is_partial, ingested_at)
                   VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                [
                    keyword, geo, date_str,
                    _safe_float(row.get("trend_index")),
                    _safe_float(row.get("search_volume")),
                    bool(row.get("is_partial", False)),
                ],
            )
            count += 1
        return count

    # ------------------------------------------------------------------
    # 采集日志
    # ------------------------------------------------------------------

    def log_collection(self, **kwargs) -> None:
        """记录一次采集任务."""
        self.conn.execute(
            """INSERT INTO curated.collection_log
               (source, domain, asins_requested, asins_succeeded,
                rows_ingested, tokens_before, tokens_after, tokens_consumed,
                duration_seconds, raw_file_path, error_message, started_at, finished_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
            [
                kwargs.get("source"),
                kwargs.get("domain"),
                kwargs.get("asins_requested", 0),
                kwargs.get("asins_succeeded", 0),
                kwargs.get("rows_ingested", 0),
                kwargs.get("tokens_before"),
                kwargs.get("tokens_after"),
                kwargs.get("tokens_consumed"),
                kwargs.get("duration_seconds"),
                kwargs.get("raw_file_path"),
                kwargs.get("error_message"),
                kwargs.get("started_at"),
            ],
        )

    # ------------------------------------------------------------------
    # 查询辅助
    # ------------------------------------------------------------------

    def get_registry_stats(self, domain: int | None = None) -> dict:
        """获取 ASIN 注册表统计."""
        where = "WHERE domain = ?" if domain else ""
        params = [domain] if domain else []

        total = self.conn.execute(
            f"SELECT COUNT(*) FROM curated.keepa_asin_registry {where}", params
        ).fetchone()[0]

        never_fetched = self.conn.execute(
            f"SELECT COUNT(*) FROM curated.keepa_asin_registry {where} "
            + (" AND " if where else " WHERE ") + "last_fetched_at IS NULL",
            params,
        ).fetchone()[0]

        history_rows = self.conn.execute(
            "SELECT COUNT(*) FROM curated.keepa_product_history"
            + (f" WHERE domain = ?" if domain else ""),
            params,
        ).fetchone()[0]

        trends_rows = self.conn.execute(
            "SELECT COUNT(*) FROM curated.google_trends_daily"
        ).fetchone()[0]

        return {
            "total_asins": total,
            "never_fetched": never_fetched,
            "fetched": total - never_fetched,
            "history_rows": history_rows,
            "trends_rows": trends_rows,
        }

    def export_history_csv(
        self,
        output_path: str | Path,
        *,
        asin: str | None = None,
        domain: int = 1,
    ) -> int:
        """导出历史数据到 CSV."""
        if pd is None:
            raise ImportError("pandas is required for CSV export")

        where_parts = ["domain = ?"]
        params: list[Any] = [domain]
        if asin:
            where_parts.append("asin = ?")
            params.append(asin)

        query = f"""
            SELECT h.*, r.product_title, r.brand, r.category, r.marketplace
            FROM curated.keepa_product_history h
            LEFT JOIN curated.keepa_asin_registry r
              ON h.asin = r.asin AND h.domain = r.domain
            WHERE {' AND '.join(where_parts)}
            ORDER BY h.asin, h.date
        """
        df = self.conn.execute(query, params).fetchdf()
        path = Path(output_path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return len(df)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _safe_float(value: Any) -> float | None:
    if value is None or value == "" or value == "null":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    if value is None or value == "" or value == "null":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None
