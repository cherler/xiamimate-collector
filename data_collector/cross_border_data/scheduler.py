"""自动化调度器: 定时采集 Keepa 数据.

工作流 (多站点模式):
  外层: 逐个 domain (US → GB → DE → FR → JP → ...)
  内层: 对当前 domain, 循环执行直到所有 L1 类目完成:
    1. 检查 token 余量
    2. ASIN 发现: 取下一个未采集的 L1 类目 → BestSeller API → 注册 top 100 ASIN
    3. 历史采集: 批量拉取 pending ASIN 的 history (token 不足时等待恢复)
    4. Google Trends: 从标题提取关键词 → pytrends 采集 (免费)
    5. 检查该 domain 剩余 L1 类目数, 若 > 0 则回到步骤 1
  当前 domain 全部 L1 类目采完后 → 切换下一个 domain

token 预算说明 (21 token/min 套餐):
- 每分钟生成 21 token, 桶容量 = 21 × 60 = 1260 token
- 策略: 先拉 BestSeller (50 token), 大量剩余 token 批量拉 history
    - bestsellers: 50 token / 次 (返回品类完整排行榜, 截取 top 100)
    - history:    2 token / ASIN (history=1 + rating=1)
    - 每个 L1 类目: 50 (bestseller) + 200 (100 ASIN × 2) = 250 token
    - 25 L1 × 250 = 6250 token ≈ 5 小时
"""

from __future__ import annotations

import csv
import fcntl
import gzip
import json
import logging
import os
from contextlib import contextmanager
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit
from urllib.request import Request, urlopen
from typing import Any

from .asin_discovery import (
    KeepaAsinDiscovery,
    SEARCH_PRODUCTS_TOKENS_PER_PAGE,
    extract_keywords_from_title,
    load_seed_asins,
)
from .business_scoring import refresh_domain_business_priorities
from .collectors.product import (
    KEEPA_DOMAIN_TO_GEO,
    KeepaCollector,
    normalize_keepa_product_snapshot,
)
from .expansion_jobs import ExpansionJob, ExpansionJobStore
from .seller_scope import evaluate_seller_scope, filter_seller_scope_keywords
from .storage import DuckDBStorage
from .token_allocator import KeepaTokenAllocator

logger = logging.getLogger(__name__)

_SHUTDOWN_REQUESTED = False


def _install_shutdown_handler() -> None:
    signal.signal(signal.SIGTERM, _request_shutdown)


def _request_shutdown(signum, frame) -> None:
    global _SHUTDOWN_REQUESTED
    _SHUTDOWN_REQUESTED = True
    try:
        signal_name = signal.Signals(signum).name
    except ValueError:
        signal_name = str(signum)
    logger.info("=== 收到 %s, 正在退出 ===", signal_name)
    raise KeyboardInterrupt


def _raise_if_shutdown_requested() -> None:
    if _SHUTDOWN_REQUESTED:
        raise KeyboardInterrupt


def _sleep_until_shutdown_or_timeout(seconds: float) -> None:
    deadline = time.time() + max(0.0, seconds)
    while True:
        _raise_if_shutdown_requested()
        remaining = deadline - time.time()
        if remaining <= 0:
            return
        time.sleep(min(1.0, remaining))


def _utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


@contextmanager
def _duckdb_access_lock():
    lock_path_raw = os.environ.get("XIAMIMATE_DUCKDB_ACCESS_LOCK_FILE", "").strip()
    if not lock_path_raw:
        yield
        return

    lock_path = Path(lock_path_raw).expanduser()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    timeout_seconds = max(0, int(os.environ.get("XIAMIMATE_DUCKDB_ACCESS_LOCK_TIMEOUT_SECONDS", "900") or "0"))
    started_at = time.time()
    with lock_path.open("a+", encoding="utf-8") as handle:
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                handle.seek(0)
                handle.truncate()
                handle.write(f"pid={os.getpid()}\nrole=auto_collect\nacquired_at={_utc_now_str()}\n")
                handle.flush()
                break
            except BlockingIOError:
                if timeout_seconds and time.time() - started_at >= timeout_seconds:
                    raise TimeoutError(f"等待 DuckDB access lock 超时: {lock_path}")
                time.sleep(2)

        try:
            yield
        finally:
            try:
                handle.seek(0)
                handle.truncate()
                handle.flush()
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


# ---------------------------------------------------------------------------
# 默认分类列表: 从 doc/amazon_{geo}_category_tree.csv 自动加载
# ---------------------------------------------------------------------------

# CSV 目录 (相对于项目根目录)
_DOC_DIR = Path(__file__).resolve().parents[2] / "doc"
_CATEGORY_CSV = _DOC_DIR / "amazon_us_category_tree.csv"  # 兼容旧引用

# 产品数低于此阈值的类目不参与轮换 (过滤掉极小或空类目)
_MIN_PRODUCT_COUNT = 50000

# 所有已支持的采集 domain
ALL_DOMAINS = sorted(KEEPA_DOMAIN_TO_GEO.keys())  # [1,2,3,4,5,6,8,9,10,11,12,13]


def _get_category_csv_for_domain(domain: int) -> Path:
    """根据 domain 返回对应的类目 CSV 路径."""
    geo = KEEPA_DOMAIN_TO_GEO.get(domain, "us").lower()
    return _DOC_DIR / f"amazon_{geo}_category_tree.csv"

# 排除虚拟/数字类目 (不适合 BestSeller 商品采集)
_EXCLUDED_CATEGORY_IDS = {
    163856011,    # Digital Music
    133140011,    # Kindle Store
    2350149011,   # Apps & Games
    18145289011,  # Audible Books & Originals
    283155,       # Books (US)
    9013971011,   # Video Shorts
    13727921011,  # Alexa Skills
    2858778011,   # Prime Video
    599858,       # Magazine Subscriptions
    3561432011,   # Credit & Payment Cards
    19419898011,  # Amazon Explore
    14297978011,  # Online Learning
    2625373011,   # Movies & TV
    229534,       # Software
    300435,       # Software (GB)
    301927,       # Software (DE)
    412612031,    # Software (IT)
    599376031,    # Software (ES)
    976451031,    # Software (IN)
    3198021,      # Software (CA)
    9482690011,   # Software (MX)
    4852502051,   # Software (AU)
    917972,       # Movies & TV (CA)
    976416031,    # Movies & TV Shows (IN)
    4852264051,   # Movies & TV (AU)
    5174,         # CDs & Vinyl
    2238192011,   # Gift Cards
    16333372011,  # Amazon Devices & Accessories
    10677469011,  # Amazon Autos
    18981045011,  # Amazon Luxury
}


def _load_categories_from_csv(csv_path: Path = _CATEGORY_CSV,
                               min_products: int = _MIN_PRODUCT_COUNT) -> list[int]:
    """从 amazon_us_category_tree.csv 读取 category_id, 按 product_count 降序.

    只返回 product_count >= min_products 的类目, 过滤掉数字音乐/Kindle 等
    不适合 BestSeller 采集的虚拟类目.
    """
    if not csv_path.exists():
        logger.warning(f"类目 CSV 不存在: {csv_path}, 使用内置默认列表")
        return []
    try:
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = []
            for row in reader:
                # strip keys for Windows \r safety
                row = {k.strip(): v for k, v in row.items()}
                cat_id = int(row["category_id"])
                count = int(row.get("product_count") or 0)
                scope_decision = evaluate_seller_scope(
                    category_name=" ".join(
                        value for value in [row.get("category_en"), row.get("category_cn")] if value
                    )
                )
                if count >= min_products and cat_id not in _EXCLUDED_CATEGORY_IDS and scope_decision.allowed:
                    rows.append((cat_id, count))
            # 按 product_count 降序
            rows.sort(key=lambda x: x[1], reverse=True)
            ids = [r[0] for r in rows]
            logger.info(f"从 CSV 加载 {len(ids)} 个类目 (product_count >= {min_products})")
            return ids
    except Exception as e:
        logger.warning(f"读取类目 CSV 失败: {e}, 使用内置默认列表")
        return []


# 硬编码兜底 (CSV 不存在时使用)
_FALLBACK_CATEGORIES = [
    172282,       # Electronics
    1055398,      # Home & Kitchen
    3375251,      # Sports & Outdoors
    7141123011,   # Clothing, Shoes & Jewelry
    3760911,      # Beauty & Personal Care
    228013,       # Tools & Home Improvement
    165793011,    # Toys & Games
    2619533011,   # Pet Supplies
]

DEFAULT_US_CATEGORIES = _load_categories_from_csv() or _FALLBACK_CATEGORIES


def _load_categories_for_domain(domain: int) -> list[int]:
    """根据 domain 加载对应类目列表."""
    csv_path = _get_category_csv_for_domain(domain)
    cats = _load_categories_from_csv(csv_path)
    if cats:
        return cats
    if domain == 1:
        return _FALLBACK_CATEGORIES
    return []


class AutoCollector:
    """把 ASIN 发现 → 历史采集 → 关键词提取 → Google Trends 串成一个自动化流水线."""

    def __init__(
        self,
        *,
        keepa_base_url: str = "https://api.keepa.com/product",
        keepa_api_key: str,
        db_path: str | Path | None = None,
        domain: int = 1,
        min_tokens_reserve: int = 2,
        tokens_per_history: int = 2,
        stale_hours: int = 1440,
        batch_size: int = 50,
        enable_google_trends: bool = True,
        enable_strategy_expansion: bool = False,
        strategy_pending_threshold: int = 200,
        strategy_category_limit: int = 2,
        strategy_keyword_limit: int = 5,
        strategy_category_cooldown_hours: int = 24 * 30,
        strategy_keyword_cooldown_hours: int = 72,
        seed_file: str | Path | None = None,
        categories: list[int] | None = None,
        search_terms: list[str] | None = None,
    ) -> None:
        self.keepa_base_url = keepa_base_url
        self.keepa_api_key = keepa_api_key
        self.domain = domain
        self.min_tokens_reserve = min_tokens_reserve
        self.tokens_per_history = tokens_per_history
        self.stale_hours = stale_hours
        self.batch_size = batch_size
        self.enable_google_trends = enable_google_trends
        self.enable_strategy_expansion = enable_strategy_expansion
        self.strategy_pending_threshold = strategy_pending_threshold
        self.strategy_category_limit = strategy_category_limit
        self.strategy_keyword_limit = strategy_keyword_limit
        self.strategy_category_cooldown_hours = strategy_category_cooldown_hours
        self.strategy_keyword_cooldown_hours = strategy_keyword_cooldown_hours
        try:
            self.business_priority_refresh_interval_seconds = max(
                0,
                int(os.environ.get("AUTO_BUSINESS_PRIORITY_REFRESH_INTERVAL_SECONDS", "21600") or "0"),
            )
        except ValueError:
            self.business_priority_refresh_interval_seconds = 21600
        try:
            self.history_max_batches_per_run = max(
                0,
                int(os.environ.get("AUTO_HISTORY_MAX_BATCHES_PER_RUN", "0") or "0"),
            )
        except ValueError:
            self.history_max_batches_per_run = 0
        try:
            self.google_trends_batch_size = max(
                1,
                min(5, int(os.environ.get("AUTO_GOOGLE_TRENDS_BATCH_SIZE", "5") or "5")),
            )
        except ValueError:
            self.google_trends_batch_size = 5
        try:
            self.google_trends_max_batches_per_run = max(
                0,
                int(os.environ.get("AUTO_GOOGLE_TRENDS_MAX_BATCHES_PER_RUN", "0") or "0"),
            )
        except ValueError:
            self.google_trends_max_batches_per_run = 0
        try:
            self.google_trends_max_keywords_per_run = max(
                1,
                int(os.environ.get("AUTO_GOOGLE_TRENDS_MAX_KEYWORDS_PER_RUN", "20") or "20"),
            )
        except ValueError:
            self.google_trends_max_keywords_per_run = 20
        try:
            self.google_trends_request_interval_seconds = max(
                0.0,
                float(os.environ.get("AUTO_GOOGLE_TRENDS_REQUEST_INTERVAL_SECONDS", "0") or "0"),
            )
        except ValueError:
            self.google_trends_request_interval_seconds = 0.0
        try:
            self.google_trends_cooldown_seconds = max(
                0,
                int(os.environ.get("AUTO_GOOGLE_TRENDS_COOLDOWN_SECONDS", "0") or "0"),
            )
        except ValueError:
            self.google_trends_cooldown_seconds = 0
        self.google_trends_mihomo_controller_url = (
            os.environ.get("AUTO_GOOGLE_TRENDS_MIHOMO_CONTROLLER_URL")
            or os.environ.get("GOOGLE_TRENDS_MIHOMO_CONTROLLER_URL")
            or ""
        ).strip().rstrip("/")
        self.google_trends_mihomo_switch_group = (
            os.environ.get("AUTO_GOOGLE_TRENDS_MIHOMO_SWITCH_GROUP")
            or os.environ.get("GOOGLE_TRENDS_MIHOMO_SWITCH_GROUP")
            or ""
        ).strip()
        self.google_trends_mihomo_secret = (
            os.environ.get("AUTO_GOOGLE_TRENDS_MIHOMO_SECRET")
            or os.environ.get("GOOGLE_TRENDS_MIHOMO_SECRET")
            or ""
        ).strip()
        log_dir = Path(os.environ.get("XIAMIMATE_LOG_DIR", "logs")).expanduser()
        self.business_priority_refresh_state_file = (
            log_dir / f"business_priority_refresh_domain_{self.domain}.state"
        )
        self.google_trends_cooldown_state_file = log_dir / f"google_trends_cooldown_domain_{self.domain}.state"
        self.seed_file = seed_file
        self.categories = categories or _load_categories_for_domain(domain)
        self.search_terms = search_terms or []

        self.storage = DuckDBStorage(db_path)
        self.discovery = KeepaAsinDiscovery(
            base_url=keepa_base_url,
            api_key=keepa_api_key,
        )
        self.collector = KeepaCollector(
            base_url=keepa_base_url,
            api_key=keepa_api_key,
        )
        self.token_allocator = KeepaTokenAllocator.from_env()
        self.expansion_job_store = ExpansionJobStore()

        # 首次运行: 从 CSV 同步类目到 DuckDB category_registry
        self._ensure_category_registry()

        # Google Trends 关键词队列: 跨多次 token 等待持续消化
        self._trends_keyword_queue: list[str] = []
        self._trends_fetched_keywords: set[str] = set()  # 本轮已采集, 避免重复
        self._trends_collector = None  # lazy init

        # 运行统计
        self._stats: dict[str, Any] = {
            "asins_discovered": 0,
            "asins_fetched": 0,
            "asins_deactivated": 0,
            "history_rows_ingested": 0,
            "snapshot_rows_ingested": 0,
            "business_scores_updated": 0,
            "business_tier_stats": {},
            "trends_rows_ingested": 0,
            "strategy_categories_selected": 0,
            "strategy_keywords_selected": 0,
            "strategy_asins_discovered": 0,
            "tokens_start": 0,
            "tokens_end": 0,
            "interactive_expansion_pending": False,
            "errors": [],
        }

    def _ensure_category_registry(self) -> None:
        """首次运行时, 从 CSV 同步类目到 DuckDB keepa_category_registry."""
        cat_stats = self.storage.get_category_stats(self.domain)
        if cat_stats["total_categories"] > 0:
            geo = KEEPA_DOMAIN_TO_GEO.get(self.domain, "?")
            logger.info(
                f"类目注册表 (domain={self.domain}/{geo}): "
                f"{cat_stats['total_categories']} 个类目, "
                f"已至少采过 BestSeller {cat_stats['bestseller_fetched']} 个, "
                f"到期待刷新/未采集 {cat_stats['bestseller_pending']} 个"
            )
            return

        # 表为空: 从 CSV 导入
        csv_path = _get_category_csv_for_domain(self.domain)
        if csv_path.exists():
            new_count = self.storage.sync_categories_from_csv(
                csv_path=csv_path,
                domain=self.domain,
                excluded_ids=_EXCLUDED_CATEGORY_IDS,
                min_products=_MIN_PRODUCT_COUNT,
            )
            geo = KEEPA_DOMAIN_TO_GEO.get(self.domain, "?")
            logger.info(f"从 CSV 导入 {new_count} 个类目到 category_registry (domain={self.domain}/{geo})")
        else:
            logger.warning(f"类目 CSV 不存在: {csv_path}")

    def run(self) -> dict[str, Any]:
        """执行完整的自动化采集流程.

        Returns
        -------
        dict
            运行统计信息.
        """
        start_time = time.time()
        logger.info("=== 自动采集开始 ===")

        try:
            # 0. 检查 token
            token_info = self.collector.check_token_status()
            tokens_left = token_info.get("tokens_left", 0)
            self._stats["tokens_start"] = tokens_left
            logger.info(f"当前 token 余量: {tokens_left}")
            interactive_pending = self.token_allocator.has_pending_interactive_jobs()
            self._stats["interactive_expansion_pending"] = interactive_pending
            if interactive_pending:
                logger.info("检测到交互式补池任务排队, auto-collect 将保留 token 并限制 history 消耗")

            # 0.2 交互式补池任务优先于后台 auto-collect discovery/history。
            self._run_interactive_expansion_job(tokens_left=tokens_left)
            if self._stats.get("interactive_expansion_waiting_token"):
                logger.info("补池任务等待 token, 本轮跳过后台采集阶段以保留 token 并快速释放 DuckDB 锁")
                return self._finish(start_time)

            # 0.5 自动停用不活跃 ASIN
            self._auto_deactivate()

            # 1. ASIN 发现 (BestSeller 需要 50 token, token 不足时跳过发现但不阻止 Phase 2)
            self._discover_asins()

            # 2. 采集历史数据 (智能等待: token 不足时等待恢复而非退出)
            self._fetch_histories()

            # 2.5 基于最新历史与快照刷新业务优先级
            self._refresh_business_priorities()

            # 3. Google Trends (免费, 但有频率限制)
            if self.enable_google_trends:
                self._fetch_google_trends()

            # 3.5 下一阶段自动扩张: L2/L3/L4 shortlist + Google Trends keyword
            self._run_strategy_expansion()

        except Exception as e:
            logger.error(f"采集过程出错: {e}", exc_info=True)
            self._stats["errors"].append(str(e))

        return self._finish(start_time)

    def _finish(self, start_time: float) -> dict:
        duration = time.time() - start_time
        try:
            token_info = self.collector.check_token_status()
            self._stats["tokens_end"] = token_info.get("tokens_left", 0)
        except Exception:
            pass

        self._stats["duration_seconds"] = round(duration, 1)
        self._stats["tokens_consumed"] = max(
            0, self._stats["tokens_start"] - self._stats["tokens_end"]
        )

        # 写采集日志
        try:
            self.storage.log_collection(
                source="auto_collect",
                domain=self.domain,
                asins_requested=self._stats["asins_discovered"],
                asins_succeeded=self._stats["asins_fetched"],
                rows_ingested=self._stats["history_rows_ingested"],
                tokens_before=self._stats["tokens_start"],
                tokens_after=self._stats["tokens_end"],
                tokens_consumed=self._stats["tokens_consumed"],
                duration_seconds=self._stats["duration_seconds"],
                error_message="; ".join(self._stats["errors"]) or None,
                started_at=datetime.fromtimestamp(
                    time.time() - self._stats["duration_seconds"],
                    tz=timezone.utc,
                ).strftime("%Y-%m-%d %H:%M:%S"),
            )
        except Exception as e:
            logger.error(f"写入采集日志失败: {e}")

        # 附带剩余进度信息, 供外层循环判断是否继续
        try:
            cat_stats = self.storage.get_category_stats(self.domain)
            self._stats["bestseller_pending"] = cat_stats["bestseller_pending"]
        except Exception:
            self._stats["bestseller_pending"] = 0
        try:
            self._stats["asins_pending"] = self.storage.count_asins_to_fetch(
                domain=self.domain,
                stale_hours=self.stale_hours,
            )
        except Exception:
            self._stats["asins_pending"] = 0

        logger.info(
            f"=== 自动采集结束 === "
            f"耗时 {self._stats['duration_seconds']}s | "
            f"发现 {self._stats['asins_discovered']} ASIN | "
            f"采集 {self._stats['asins_fetched']} ASIN | "
            f"停用 {self._stats['asins_deactivated']} ASIN | "
            f"写入 {self._stats['history_rows_ingested']} 行 | "
            f"消耗 {self._stats['tokens_consumed']} token | "
            f"待刷新类目 {self._stats['bestseller_pending']} | "
            f"剩余 ASIN {self._stats['asins_pending']}"
        )
        return dict(self._stats)

    # ------------------------------------------------------------------
    # Phase 0.2: 交互式补池 job
    # ------------------------------------------------------------------

    def _run_interactive_expansion_job(self, *, tokens_left: int) -> None:
        if not self.expansion_job_store.enabled:
            return
        try:
            hydrate_job = self.expansion_job_store.claim_next_hydration_job(domain=self.domain)
            if hydrate_job is not None:
                self._hydrate_interactive_expansion_job(hydrate_job, tokens_left=tokens_left)
                return
            job = self.expansion_job_store.claim_next_interactive_job(domain=self.domain)
        except Exception as e:
            logger.warning(f"读取补池任务失败: {e}")
            return
        if job is None:
            return

        scope_decision = evaluate_seller_scope(
            category_path=job.category_path,
            query=job.product_query,
        )
        blocked_by_category_id = job.category_id in _EXCLUDED_CATEGORY_IDS if job.category_id is not None else False
        if blocked_by_category_id:
            message = f"seller_scope_blocked:excluded_category_id:{job.category_id}"
            self.expansion_job_store.mark_failed(job_id=job.job_id, error_message=message)
            logger.info(f"补池任务 {job.job_id} 超出中小跨境卖家经营范围, 已拦截: {message}")
            return
        if not scope_decision.allowed:
            message = (
                f"seller_scope_blocked:{scope_decision.reason_code}:"
                f"{','.join(scope_decision.matched_terms)}"
            )
            self.expansion_job_store.mark_failed(job_id=job.job_id, error_message=message)
            logger.info(f"补池任务 {job.job_id} 超出中小跨境卖家经营范围, 已拦截: {message}")
            return

        logger.info(
            f"处理补池任务 {job.job_id}: category_id={job.category_id}, "
            f"target={job.target_asin_count}, priority={job.priority}"
        )

        try:
            tokens_before = tokens_left
            if job.category_id is not None:
                discovery_cost = self.token_allocator.budget.bestseller_min_tokens
                decision = self.token_allocator.can_run(
                    queue_name="interactive",
                    tokens_left=tokens_left,
                    cost=discovery_cost,
                    interactive_pending=True,
                )
                if not decision.allowed:
                    self.expansion_job_store.mark_waiting_token(
                        job_id=job.job_id,
                        tokens_left=tokens_left,
                        reason=decision.reason,
                    )
                    self._stats["interactive_expansion_waiting_token"] = True
                    logger.info(f"补池任务 {job.job_id} 等待 token: {decision.reason}")
                    return
                all_asins, raw_payload = self.discovery.fetch_best_sellers(
                    category=job.category_id,
                    domain=self.domain,
                )
                raw_category = "expansion_bestsellers"
                raw_label = f"job_{job.job_id}_cat_{job.category_id}"
                discovery_source = "interactive_expansion_bestseller"
            elif job.product_query:
                discovery_cost = max(
                    SEARCH_PRODUCTS_TOKENS_PER_PAGE,
                    self.token_allocator.budget.search_min_tokens,
                )
                decision = self.token_allocator.can_run(
                    queue_name="interactive",
                    tokens_left=tokens_left,
                    cost=discovery_cost,
                    interactive_pending=True,
                )
                if not decision.allowed:
                    self.expansion_job_store.mark_waiting_token(
                        job_id=job.job_id,
                        tokens_left=tokens_left,
                        reason=decision.reason,
                    )
                    self._stats["interactive_expansion_waiting_token"] = True
                    logger.info(f"补池任务 {job.job_id} 等待 token: {decision.reason}")
                    return
                all_asins = self.discovery.search_products(
                    term=job.product_query,
                    domain=self.domain,
                )
                raw_payload = {"asinList": all_asins, "term": job.product_query}
                raw_category = "expansion_search"
                raw_label = f"job_{job.job_id}_search"
                discovery_source = "interactive_expansion_search"
            else:
                self.expansion_job_store.mark_failed(
                    job_id=job.job_id,
                    error_message="category_id or product_query is required for expansion discovery",
                )
                return

            target_count = max(1, min(job.target_asin_count, 100))
            asins = all_asins[:target_count]
            raw_path = _save_raw_response(
                raw_payload,
                category=raw_category,
                label=raw_label,
                domain=self.domain,
                asins=asins,
            )
            if raw_path:
                self.storage.upsert_asin_raw_file_mappings(
                    asins=asins,
                    domain=self.domain,
                    source=discovery_source,
                    raw_file_path=raw_path,
                )

            discovered = [
                {
                    "asin": asin,
                    "domain": self.domain,
                    "category_id": job.category_id,
                    "category_path": job.category_path,
                    "search_term": job.product_query,
                    "discovery_source": discovery_source,
                    "priority": 100,
                }
                for asin in asins
            ]
            new_count = self.storage.register_asins(discovered)
            if job.category_id is not None:
                self.storage.mark_category_bestseller_done(job.category_id, self.domain, len(asins))
            try:
                token_now = self.collector.check_token_status()
                tokens_after = token_now.get("tokens_left", max(0, tokens_before - discovery_cost))
            except Exception:
                tokens_after = max(0, tokens_before - discovery_cost)
            self.expansion_job_store.mark_hydrating(
                job_id=job.job_id,
                result_candidate_asins=asins,
                result_new_asin_count=new_count,
                tokens_before=tokens_before,
                tokens_after=tokens_after,
            )
            self._stats["asins_discovered"] += new_count
            logger.info(
                f"补池任务 {job.job_id}: discovery 返回 {len(all_asins)} 个 ASIN, "
                f"注册新增 {new_count} 个, 状态转为 hydrating"
            )
            hydrate_job = ExpansionJob(
                job_id=job.job_id,
                domain=job.domain,
                marketplace=job.marketplace,
                priority=job.priority,
                product_query=job.product_query,
                recall_mode=job.recall_mode,
                category_id=job.category_id,
                category_path=job.category_path,
                include_descendants=job.include_descendants,
                target_asin_count=job.target_asin_count,
                tokens_estimated=job.tokens_estimated,
                result_candidate_asins=asins,
                result_new_asin_count=new_count,
            )
            self._hydrate_interactive_expansion_job(hydrate_job, tokens_left=tokens_after)
        except Exception as e:
            logger.warning(f"补池任务 {job.job_id} 执行失败: {e}")
            self.expansion_job_store.mark_failed(job_id=job.job_id, error_message=str(e))

    def _hydrate_interactive_expansion_job(self, job: ExpansionJob, *, tokens_left: int) -> None:
        asins = list(dict.fromkeys(job.result_candidate_asins or []))
        if not asins:
            self.expansion_job_store.mark_syncing(
                job_id=job.job_id,
                result_candidate_asins=[],
                result_new_asin_count=job.result_new_asin_count,
                tokens_before=tokens_left,
                tokens_after=tokens_left,
            )
            logger.info(f"补池任务 {job.job_id}: 无候选 ASIN, 状态转为 syncing 等待完成态回写")
            return

        target_count = max(1, min(job.target_asin_count, len(asins)))
        candidate_asins = asins[:target_count]
        hydrate_asins = self.storage.filter_asins_to_fetch(
            candidate_asins,
            domain=self.domain,
            stale_hours=self.stale_hours,
        )
        if not hydrate_asins:
            self.expansion_job_store.mark_syncing(
                job_id=job.job_id,
                result_candidate_asins=asins,
                result_new_asin_count=job.result_new_asin_count,
                tokens_before=tokens_left,
                tokens_after=tokens_left,
            )
            logger.info(
                f"补池任务 {job.job_id}: {len(candidate_asins)} 个候选 ASIN 已完成 hydrate, "
                "状态转为 syncing 等待完成态回写"
            )
            return

        hydrate_cost = max(1, len(hydrate_asins) * self.tokens_per_history)
        decision = self.token_allocator.can_run(
            queue_name="interactive",
            tokens_left=tokens_left,
            cost=hydrate_cost,
            interactive_pending=True,
        )
        if not decision.allowed:
            self.expansion_job_store.mark_hydrating_waiting_token(
                job_id=job.job_id,
                tokens_left=tokens_left,
                reason=decision.reason,
            )
            self._stats["interactive_expansion_waiting_token"] = True
            logger.info(f"补池任务 {job.job_id} hydrate 等待 token: {decision.reason}")
            return

        tokens_before = tokens_left
        batch_start = time.time()
        try:
            history_rows, raw = self.collector.fetch_product_history(
                asins=hydrate_asins,
                domain=self.domain,
            )
            returned_asins = {row["asin"] for row in history_rows if row.get("asin")}
            missing_asins = [asin for asin in hydrate_asins if asin not in returned_asins]
            if missing_asins:
                self.storage.deactivate_asins(
                    [(asin, self.domain) for asin in missing_asins],
                    reason="no_data",
                )
                logger.info(f"补池任务 {job.job_id}: {len(missing_asins)} 个 ASIN 在 Keepa 中无数据, 已停用")

            ingested = self.storage.ingest_keepa_history(history_rows, domain=self.domain)
            snapshot_rows = []
            capture_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            for product in raw.get("raw_products", {}).get("products", []):
                snapshot_rows.append(
                    normalize_keepa_product_snapshot(
                        product,
                        domain=self.domain,
                        update_time=capture_time,
                        source_url=self.keepa_base_url,
                    )
                )
            snapshot_ingested = self.storage.ingest_keepa_product_snapshots(
                snapshot_rows,
                domain=self.domain,
            )

            for asin in hydrate_asins:
                self.storage.mark_fetched(asin, self.domain)

            for product in raw.get("raw_products", {}).get("products", []):
                category_tree = product.get("categoryTree")
                category_path = None
                root_category_id = None
                if category_tree and isinstance(category_tree, list) and len(category_tree) > 0:
                    names = [node.get("name", "") for node in category_tree if node.get("name")]
                    if names:
                        category_path = " > ".join(names)
                    root_category_id = category_tree[0].get("catId")
                    tree_categories = []
                    for index, node in enumerate(category_tree):
                        tree_categories.append({
                            "category_id": node.get("catId"),
                            "category_en": node.get("name"),
                            "parent_id": category_tree[index - 1].get("catId") if index > 0 else None,
                            "depth": index + 1,
                            "product_count": 0,
                        })
                    if tree_categories:
                        self.storage.upsert_categories_from_tree(tree_categories, domain=self.domain)

                self.storage.update_asin_metadata(
                    product.get("asin", ""),
                    self.domain,
                    product_title=product.get("title"),
                    brand=product.get("brand"),
                    category=product.get("productGroup"),
                    category_path=category_path,
                    root_category_id=root_category_id,
                )

                title = product.get("title", "")
                if title:
                    keywords = extract_keywords_from_title(title, max_keywords=3)
                    keywords = [keyword for keyword in keywords if len(keyword.split()) >= 2 and len(keyword) <= 50]
                    if keywords:
                        self.storage.upsert_asin_keywords(product.get("asin", ""), self.domain, keywords)

            raw_path = _save_raw_response(
                raw.get("raw_products", {}),
                category="expansion_products",
                label=f"job_{job.job_id}_hydrate",
                domain=self.domain,
                asins=hydrate_asins,
                compression="gzip",
            )
            if raw_path:
                self.storage.upsert_asin_raw_file_mappings(
                    asins=hydrate_asins,
                    domain=self.domain,
                    source="interactive_expansion_hydrate",
                    raw_file_path=raw_path,
                )

            try:
                token_now = self.collector.check_token_status()
                tokens_after = token_now.get("tokens_left", max(0, tokens_before - hydrate_cost))
            except Exception:
                tokens_after = max(0, tokens_before - hydrate_cost)

            duration_seconds = round(time.time() - batch_start, 1)
            try:
                self.storage.log_collection(
                    source="interactive_expansion_hydrate",
                    domain=self.domain,
                    asins_requested=len(hydrate_asins),
                    asins_succeeded=len(returned_asins),
                    rows_ingested=ingested,
                    tokens_before=tokens_before,
                    tokens_after=tokens_after,
                    tokens_consumed=max(0, tokens_before - tokens_after),
                    duration_seconds=duration_seconds,
                    raw_file_path=str(raw_path) if raw_path else None,
                    error_message=None,
                    started_at=datetime.fromtimestamp(batch_start, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                )
            except Exception as log_error:
                logger.warning(f"写入补池 hydrate 日志失败: {log_error}")

            self._stats["asins_fetched"] += len(hydrate_asins)
            self._stats["history_rows_ingested"] += ingested
            self._stats["snapshot_rows_ingested"] += snapshot_ingested
            self.expansion_job_store.mark_syncing(
                job_id=job.job_id,
                result_candidate_asins=asins,
                result_new_asin_count=job.result_new_asin_count,
                tokens_before=tokens_before,
                tokens_after=tokens_after,
            )
            logger.info(
                f"补池任务 {job.job_id}: hydrate {len(hydrate_asins)}/{len(candidate_asins)} 个待补水 ASIN, "
                f"写入 {ingested} 行历史/{snapshot_ingested} 行快照, 状态转为 syncing"
            )
        except Exception as e:
            logger.warning(f"补池任务 {job.job_id} hydrate 失败: {e}")
            self.expansion_job_store.mark_failed(job_id=job.job_id, error_message=str(e))

    # ------------------------------------------------------------------
    # Phase 0.5: 自动停用不活跃 ASIN
    # ------------------------------------------------------------------

    def _auto_deactivate(self) -> None:
        """扫描注册表, 自动停用不活跃 ASIN."""
        logger.info("--- Phase 0.5: 自动停用检测 ---")
        try:
            stats = self.storage.run_auto_deactivation(
                domain=self.domain, dry_run=False
            )
            total = sum(stats.values())
            self._stats["asins_deactivated"] = total
            if total > 0:
                logger.info(f"自动停用 {total} 个 ASIN: {stats}")
            else:
                logger.info("无需停用")
        except Exception as e:
            logger.warning(f"自动停用检测失败: {e}")

    # ------------------------------------------------------------------
    # Phase 1: ASIN 发现
    # ------------------------------------------------------------------

    def _discover_asins(self) -> None:
        logger.info("--- Phase 1: ASIN 发现 ---")
        all_discovered: list[dict] = []

        # 1a. 从种子文件加载
        if self.seed_file:
            seeds = load_seed_asins(self.seed_file)
            for s in seeds:
                s.setdefault("discovery_source", "seed")
            all_discovered.extend(seeds)
            logger.info(f"从种子文件加载 {len(seeds)} 个 ASIN")

        # 1b. 自动检测 DuckDB 中待采集 ASIN (未采集 / 已过期)
        pending_count = self.storage.count_asins_to_fetch(
            domain=self.domain,
            stale_hours=self.stale_hours,
        )

        interactive_pending = bool(self._stats.get("interactive_expansion_pending"))

        # 默认允许 discovery 在 pending ASIN 非空时按预算执行, 防止 history 长期占用导致发现任务饿死。
        if pending_count > 0 and not self.token_allocator.budget.allow_discovery_with_pending:
            logger.info(f"待采集池有 {pending_count} 个 ASIN, 跳过 BestSeller")
            if all_discovered:
                new_count = self.storage.register_asins(all_discovered)
                self._stats["asins_discovered"] = new_count
            return
        if pending_count > 0:
            logger.info(
                f"待采集池有 {pending_count} 个 ASIN, 但 AUTO_DISCOVERY_ALLOW_WHEN_PENDING=true, "
                "按 token 预算继续尝试 BestSeller/search discovery"
            )

        # 1c. 待采集池为空 → 从 category_registry 取下一个未读过 BestSeller 的类目
        cat_stats = self.storage.get_category_stats(self.domain)
        logger.info(
            f"类目进度: {cat_stats['bestseller_fetched']}/{cat_stats['total_categories']} 已至少采过, "
            f"当前到期待刷新/未采集 {cat_stats['bestseller_pending']} 个"
        )

        next_cat = self.storage.get_next_category_for_bestseller(self.domain)
        while next_cat is not None:
            cat_id = int(next_cat["category_id"])
            cat_name = next_cat.get("category_cn") or next_cat.get("category_en") or str(cat_id)
            scope_decision = evaluate_seller_scope(category_name=cat_name)
            if scope_decision.allowed:
                break
            self.storage.mark_category_bestseller_done(cat_id, self.domain, 0)
            logger.info(
                f"跳过超出中小跨境卖家经营范围的类目 {cat_id} ({cat_name}): "
                f"{scope_decision.reason_code} {list(scope_decision.matched_terms)}"
            )
            next_cat = self.storage.get_next_category_for_bestseller(self.domain)

        if next_cat is None:
            logger.info("当前没有到期的 L1 BestSeller 刷新类目, 无新类目可发现")
            # 注册种子 ASIN (如果有)
            if all_discovered:
                new_count = self.storage.register_asins(all_discovered)
                self._stats["asins_discovered"] = new_count
            return

        cat_id = int(next_cat["category_id"])
        cat_name = next_cat.get("category_cn") or next_cat.get("category_en") or str(cat_id)

        # BestSeller API 消耗 50 token, 先检查余量
        try:
            token_info = self.collector.check_token_status()
            tokens_left = token_info.get("tokens_left", 0)
        except Exception:
            tokens_left = 0

        decision = self.token_allocator.can_run(
            queue_name="auto_discovery",
            tokens_left=tokens_left,
            cost=self.token_allocator.budget.bestseller_min_tokens,
            interactive_pending=interactive_pending,
        )
        if not decision.allowed:
            logger.info(
                f"token 预算不足 ({decision.reason}), 无法刷新 BestSeller, 本轮暂不处理类目 {cat_id} ({cat_name})"
            )
            if all_discovered:
                new_count = self.storage.register_asins(all_discovered)
                self._stats["asins_discovered"] = new_count
            return

        logger.info(
            f"BestSeller: 类目 {cat_id} ({cat_name}), "
            f"product_count={next_cat.get('product_count')}, token={tokens_left}"
        )

        try:
            bestseller_started_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            all_asins, raw_payload = self.discovery.fetch_best_sellers(
                category=cat_id, domain=self.domain
            )
            # 只取 top 100 ASIN, 并保持原始排名顺序去重。
            asins = list(dict.fromkeys(all_asins))[:100]
            existing_count = self.storage.count_registered_asins(asins, self.domain)
            new_count = max(0, len(asins) - existing_count)
            new_rate = (new_count / len(asins)) if asins else 0.0

            # 保存 BestSeller 原始 API 响应
            raw_path = _save_raw_response(
                raw_payload,
                category="bestsellers",
                label=f"cat_{cat_id}",
                domain=self.domain,
                asins=asins,
            )
            if raw_path:
                self.storage.upsert_asin_raw_file_mappings(
                    asins=asins,
                    domain=self.domain,
                    source="bestsellers",
                    raw_file_path=raw_path,
                )

            try:
                self.storage.log_collection(
                    source="bestsellers",
                    domain=self.domain,
                    asins_requested=len(all_asins),
                    asins_succeeded=len(asins),
                    rows_ingested=0,
                    tokens_before=tokens_left,
                    tokens_after=max(0, tokens_left - 50),
                    tokens_consumed=50,
                    duration_seconds=None,
                    raw_file_path=str(raw_path) if raw_path else None,
                    error_message=None,
                    started_at=bestseller_started_at,
                )
            except Exception as e:
                logger.warning(f"写入 BestSeller 日志失败: {e}")

            logger.info(
                f"品类 {cat_id} ({cat_name}): BestSeller 返回 {len(all_asins)} 个 ASIN, "
                f"截取 top {len(asins)}, 新 ASIN {new_count}, 已存在 {existing_count}, "
                f"新占比 {new_rate:.1%}"
            )

            for asin in asins:
                all_discovered.append({
                    "asin": asin,
                    "domain": self.domain,
                    "category_id": cat_id,
                    "discovery_source": "bestseller",
                })
            # 标记该类目已采集
            self.storage.mark_category_bestseller_done(
                cat_id,
                self.domain,
                len(asins),
                new_asin_count=new_count,
                existing_asin_count=existing_count,
            )
            logger.info(
                f"品类 {cat_id} ({cat_name}): 注册 {len(asins)} 个 ASIN, "
                f"消耗 50 token"
            )
        except Exception as e:
            logger.warning(f"品类 {cat_id} ({cat_name}) BestSeller 获取失败: {e}")
            self._stats["errors"].append(f"bestseller_{cat_id}: {e}")

        # 1d. 关键词搜索 (Keepa 定义: 10 token / 结果页)
        scoped_terms, blocked_terms = filter_seller_scope_keywords(self.search_terms)
        for blocked in blocked_terms:
            logger.info(
                f"跳过超出中小跨境卖家经营范围的搜索词: "
                f"{blocked.reason_code} {list(blocked.matched_terms)}"
            )

        for term in scoped_terms:
            try:
                token_info = self.collector.check_token_status()
                decision = self.token_allocator.can_run(
                    queue_name="auto_discovery",
                    tokens_left=token_info.get("tokens_left", 0),
                    cost=max(SEARCH_PRODUCTS_TOKENS_PER_PAGE, self.token_allocator.budget.search_min_tokens),
                    interactive_pending=interactive_pending,
                )
                if not decision.allowed:
                    logger.warning(
                        f"token 预算不足 ({decision.reason}), 跳过剩余搜索"
                    )
                    break

                asins = self.discovery.search_products(
                    term=term, domain=self.domain
                )
                for asin in asins:
                    all_discovered.append({
                        "asin": asin,
                        "domain": self.domain,
                        "search_term": term,
                        "discovery_source": "search",
                    })
                logger.info(f"搜索 '{term}': 发现 {len(asins)} 个 ASIN")
            except Exception as e:
                logger.warning(f"搜索 '{term}' 失败: {e}")
                self._stats["errors"].append(f"search_{term}: {e}")

        # 去重后注册到 DuckDB
        if all_discovered:
            new_count = self.storage.register_asins(all_discovered)
            self._stats["asins_discovered"] = new_count
            logger.info(f"注册 {new_count} 个新 ASIN (总发现 {len(all_discovered)} 个, 去重后新增 {new_count})")

    # ------------------------------------------------------------------
    # Phase 2: 采集历史数据
    # ------------------------------------------------------------------

    def _fetch_histories(self) -> None:
        """采集 Keepa 历史数据.

        智能等待策略: token 不足时不退出, 而是根据 refillIn 等待恢复后继续,
        直到所有待采集 ASIN 全部完成.
        """
        logger.info("--- Phase 2: 采集 Keepa 历史数据 ---")
        total_fetched = 0
        total_rows = 0
        batches_fetched = 0
        consecutive_errors = 0
        max_consecutive_errors = 5

        while True:
            # 检查 token
            try:
                token_info = self.collector.check_token_status()
                tokens_left = token_info.get("tokens_left", 0)
                refill_in_ms = token_info.get("refill_in_ms", 60000)
                consecutive_errors = 0  # 重置错误计数
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"检查 token 失败 ({consecutive_errors}/{max_consecutive_errors}): {e}")
                if consecutive_errors >= max_consecutive_errors:
                    logger.error("连续失败过多, 停止采集")
                    break
                time.sleep(30)
                continue

            # token 不足: 等待恢复而非退出
            if tokens_left < self.tokens_per_history:
                # 先看还有没有待采集的 ASIN
                pending_check = self.storage.get_asins_to_fetch(
                    domain=self.domain, max_count=1, stale_hours=self.stale_hours,
                )
                if not pending_check:
                    logger.info("没有待采集的 ASIN, 结束")
                    break

                # 计算等待时间: refillIn 是下一个 token 恢复的毫秒数
                wait_secs = max((refill_in_ms / 1000) + 1, 10)
                wait_secs = min(wait_secs, 300)  # 最多等 5 分钟 (防止异常值)
                logger.info(
                    f"token 不足 ({tokens_left}), "
                    f"等待 {wait_secs:.0f}s 后恢复 (还有待采集 ASIN)"
                )
                # 利用等待时间采集免费的 Google Trends
                self._fetch_trends_during_wait(wait_secs)
                continue

            interactive_pending = self.token_allocator.has_pending_interactive_jobs()
            history_budget = self.token_allocator.history_token_budget(
                tokens_left=tokens_left,
                interactive_pending=interactive_pending,
            )
            if not history_budget.allowed:
                logger.info(f"history 采集暂停: {history_budget.reason}")
                break

            # 动态计算本批可采集数量: min(max_batch_size, token余量/每ASIN消耗)
            max_asins_by_token = history_budget.tokens_available_for_queue // self.tokens_per_history
            batch_limit = min(self.batch_size, max_asins_by_token)
            if batch_limit <= 0:
                time.sleep(60)
                continue

            # 从注册表中取待采集 ASIN
            pending = self.storage.get_asins_to_fetch(
                domain=self.domain,
                max_count=batch_limit,
                stale_hours=self.stale_hours,
            )
            if not pending:
                logger.info("没有待采集的 ASIN, 结束")
                break

            asin_list = [p["asin"] for p in pending]
            logger.info(f"本批采集 {len(asin_list)} 个 ASIN, token 余量 {tokens_left}")
            batch_start = time.time()
            tokens_before_batch = tokens_left

            try:
                t0 = time.time()
                history_rows, raw = self.collector.fetch_product_history(
                    asins=asin_list,
                    domain=self.domain,
                )
                t_api = time.time()

                # 检测哪些 ASIN 没有返回数据 (Keepa 查无此商品)
                returned_asins = {r["asin"] for r in history_rows if r.get("asin")}
                missing_asins = [a for a in asin_list if a not in returned_asins]
                if missing_asins:
                    self.storage.deactivate_asins(
                        [(a, self.domain) for a in missing_asins],
                        reason="no_data",
                    )
                    logger.info(f"  {len(missing_asins)} 个 ASIN 在 Keepa 中无数据, 已停用")

                # 入库
                ingested = self.storage.ingest_keepa_history(
                    history_rows, domain=self.domain
                )
                t_history = time.time()
                snapshot_rows = []
                capture_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                for product in raw.get("raw_products", {}).get("products", []):
                    snapshot_rows.append(
                        normalize_keepa_product_snapshot(
                            product,
                            domain=self.domain,
                            update_time=capture_time,
                            source_url=self.keepa_base_url,
                        )
                    )
                snapshot_ingested = self.storage.ingest_keepa_product_snapshots(
                    snapshot_rows,
                    domain=self.domain,
                )
                total_rows += ingested
                total_fetched += len(asin_list)
                self._stats["snapshot_rows_ingested"] += snapshot_ingested

                # 标记已采集 + 更新元数据
                for asin in asin_list:
                    self.storage.mark_fetched(asin, self.domain)

                # 从返回数据中更新商品标题、类目路径等元数据
                for product in raw.get("raw_products", {}).get("products", []):
                    cat_tree = product.get("categoryTree")
                    category_path = None
                    root_category_id = None
                    if cat_tree and isinstance(cat_tree, list) and len(cat_tree) > 0:
                        names = [n.get("name", "") for n in cat_tree if n.get("name")]
                        if names:
                            category_path = " > ".join(names)
                        root_category_id = cat_tree[0].get("catId")

                        # 自动注册 categoryTree 中发现的类目到 category_registry
                        tree_cats = []
                        for i, node in enumerate(cat_tree):
                            tree_cats.append({
                                "category_id": node.get("catId"),
                                "category_en": node.get("name"),
                                "parent_id": cat_tree[i - 1].get("catId") if i > 0 else None,
                                "depth": i + 1,
                                "product_count": 0,
                            })
                        if tree_cats:
                            self.storage.upsert_categories_from_tree(
                                tree_cats, domain=self.domain
                            )

                    self.storage.update_asin_metadata(
                        product.get("asin", ""),
                        self.domain,
                        product_title=product.get("title"),
                        brand=product.get("brand"),
                        category=product.get("productGroup"),
                        category_path=category_path,
                        root_category_id=root_category_id,
                    )

                    # 提取关键词并保存 asin↔keyword 映射
                    title = product.get("title", "")
                    if title:
                        kws = extract_keywords_from_title(title, max_keywords=3)
                        kws = [kw for kw in kws if len(kw.split()) >= 2 and len(kw) <= 50]
                        if kws:
                            self.storage.upsert_asin_keywords(
                                product.get("asin", ""), self.domain, kws
                            )

                # 保存原始 API 响应到本地
                raw_path = _save_raw_response(
                    raw.get("raw_products", {}),
                    category="products",
                    label=f"batch_{total_fetched}",
                    domain=self.domain,
                    asins=asin_list,
                    compression="gzip",
                )
                if raw_path:
                    self.storage.upsert_asin_raw_file_mappings(
                        asins=asin_list,
                        domain=self.domain,
                        source="auto_collect",
                        raw_file_path=raw_path,
                    )

                t_post = time.time()
                logger.info(
                    f"  写入 {ingested} 行历史数据, {snapshot_ingested} 行快照数据"
                    f" (API {t_api - t0:.1f}s, 入库 {t_history - t_api:.1f}s,"
                    f" 后处理 {t_post - t_history:.1f}s)"
                )
                consecutive_errors = 0

                # 每批写一条采集日志, 供 Grafana 实时监控
                try:
                    token_now = self.collector.check_token_status()
                    tokens_after_batch = token_now.get("tokens_left", 0)
                except Exception:
                    tokens_after_batch = tokens_before_batch
                batch_duration = round(time.time() - batch_start, 1)
                try:
                    self.storage.log_collection(
                        source="auto_collect",
                        domain=self.domain,
                        asins_requested=len(asin_list),
                        asins_succeeded=len(returned_asins),
                        rows_ingested=ingested,
                        tokens_before=tokens_before_batch,
                        tokens_after=tokens_after_batch,
                        tokens_consumed=max(0, tokens_before_batch - tokens_after_batch),
                        duration_seconds=batch_duration,
                        raw_file_path=str(raw_path) if raw_path else None,
                        error_message=None,
                        started_at=datetime.fromtimestamp(
                            batch_start, tz=timezone.utc
                        ).strftime("%Y-%m-%d %H:%M:%S"),
                    )
                except Exception as e:
                    logger.warning(f"写入批次日志失败: {e}")

                try:
                    checkpoint_start = time.time()
                    self.storage.checkpoint()
                    logger.info(f"  DuckDB checkpoint 完成 ({time.time() - checkpoint_start:.1f}s)")
                except Exception as e:
                    logger.warning(f"DuckDB checkpoint 失败: {e}")

                batches_fetched += 1
                if self.history_max_batches_per_run and batches_fetched >= self.history_max_batches_per_run:
                    logger.info(
                        "history 本轮已处理 %s/%s 批，释放 DuckDB 锁给同步任务",
                        batches_fetched,
                        self.history_max_batches_per_run,
                    )
                    break

            except Exception as e:
                consecutive_errors += 1
                logger.error(f"采集 {asin_list[:3]}... 失败 ({consecutive_errors}/{max_consecutive_errors}): {e}")
                self._stats["errors"].append(f"history: {e}")
                if consecutive_errors >= max_consecutive_errors:
                    logger.error("连续采集失败过多, 停止")
                    break
                time.sleep(30)

        self._stats["asins_fetched"] = total_fetched
        self._stats["history_rows_ingested"] = total_rows

    def _refresh_business_priorities(self) -> None:
        logger.info("--- Phase 2.5: 刷新业务评分与调度优先级 ---")
        try:
            interval_seconds = self.business_priority_refresh_interval_seconds
            if interval_seconds > 0:
                age_seconds = None
                try:
                    age_seconds = time.time() - self.business_priority_refresh_state_file.stat().st_mtime
                except FileNotFoundError:
                    age_seconds = None
                if age_seconds is not None and age_seconds < interval_seconds:
                    wait_seconds = max(0, interval_seconds - int(age_seconds))
                    logger.info(
                        f"业务评分跳过: 距上次刷新 {int(age_seconds)}s, "
                        f"未达到最小间隔 {interval_seconds}s, 约 {wait_seconds}s 后可刷新"
                    )
                    return

            result = refresh_domain_business_priorities(
                self.storage,
                domain=self.domain,
            )
            self._stats["business_scores_updated"] = result.get("scored", 0)
            self._stats["business_tier_stats"] = result.get("tiers", {})
            logger.info(
                f"业务评分已刷新: {self._stats['business_scores_updated']} 个 ASIN, "
                f"分层 {self._stats['business_tier_stats']}"
            )
            if interval_seconds > 0:
                self.business_priority_refresh_state_file.parent.mkdir(parents=True, exist_ok=True)
                self.business_priority_refresh_state_file.write_text(_utc_now_str() + "\n", encoding="utf-8")
        except Exception as e:
            logger.warning(f"刷新业务评分失败: {e}")
            self._stats["errors"].append(f"business_score: {e}")

    @staticmethod
    def _strategy_priority(base_priority: int, score: float | int) -> int:
        return max(0, min(100, int(round(base_priority + float(score) * 5))))

    def _run_strategy_expansion(self) -> None:
        logger.info("--- Phase 3.5: 下一阶段自动扩张 ---")
        if not self.enable_strategy_expansion:
            logger.info("未启用 strategy expansion, 跳过")
            return

        pending_count = self.storage.count_asins_to_fetch(
            domain=self.domain,
            stale_hours=self.stale_hours,
        )
        if pending_count > self.strategy_pending_threshold:
            logger.info(
                f"待采集池总数仍有 {pending_count} 个 ASIN (> {self.strategy_pending_threshold}), 暂不做 L2/L3/L4 / keyword 扩张"
            )
            return

        try:
            self._expand_shortlist_categories()
        except Exception:
            logger.exception("L2/L3/L4 shortlist 扩张异常, 继续执行 keyword 扩张")
        try:
            self._expand_keywords()
        except Exception:
            logger.exception("keyword 扩张异常")

    def _expand_shortlist_categories(self) -> None:
        candidates = self.storage.get_subcategory_expansion_candidates(
            domain=self.domain,
            limit=self.strategy_category_limit,
            cooldown_hours=self.strategy_category_cooldown_hours,
        )
        if not candidates:
            logger.info("未找到可扩张的 L2/L3/L4 shortlist 类目")
            return

        logger.info(f"L2/L3/L4 shortlist 候选 {len(candidates)} 个")
        for index, candidate in enumerate(candidates):
            try:
                token_info = self.collector.check_token_status()
                tokens_before = token_info.get("tokens_left", 0)
            except Exception:
                tokens_before = 0

            if tokens_before < 50 + self.min_tokens_reserve:
                logger.info(f"token 不足 ({tokens_before})，跳过剩余 L2/L3/L4 扩张")
                logger.info("本轮未执行的 L2/L3/L4 shortlist 候选不会写入 cooldown；后续 token 恢复后会重新参与扩张")
                try:
                    self.storage.log_collection(
                        source="strategy_category_skip",
                        domain=self.domain,
                        asins_requested=len(candidates) - index,
                        asins_succeeded=0,
                        rows_ingested=0,
                        tokens_before=tokens_before,
                        tokens_after=tokens_before,
                        tokens_consumed=0,
                        duration_seconds=None,
                        raw_file_path=None,
                        error_message="token_insufficient",
                        started_at=_utc_now_str(),
                    )
                except Exception as e:
                    logger.warning(f"写入 L2/L3/L4 token skip 日志失败: {e}")
                break

            category_id = int(candidate["category_id"])
            category_name = candidate.get("category_name") or str(category_id)
            category_path = candidate.get("category_path")
            scope_decision = evaluate_seller_scope(
                category_path=category_path,
                category_name=category_name,
            )
            if not scope_decision.allowed:
                logger.info(
                    f"跳过超出中小跨境卖家经营范围的 L2/L3/L4 类目 "
                    f"{category_id} ({category_name}): {scope_decision.reason_code} "
                    f"{list(scope_decision.matched_terms)}"
                )
                self.storage.record_discovery_expansion(
                    expansion_type="category",
                    domain=self.domain,
                    target_key=str(category_id),
                    target_label=str(category_name),
                    priority_score=0,
                    candidate_count=0,
                    new_asin_count=0,
                    notes=(
                        f"seller_scope_blocked={scope_decision.reason_code};"
                        f"matched_terms={','.join(scope_decision.matched_terms)}"
                    ),
                )
                continue
            shortlist_score = float(candidate.get("shortlist_score") or 0)
            started_at = _utc_now_str()
            raw_path = None
            new_count = 0
            requested = 0
            error_message = None

            try:
                all_asins, raw_payload = self.discovery.fetch_best_sellers(
                    category=category_id,
                    domain=self.domain,
                )
                asins = all_asins[:100]
                requested = len(asins)
                priority = self._strategy_priority(65, shortlist_score)
                discovered_rows = [
                    {
                        "asin": asin,
                        "domain": self.domain,
                        "category_id": category_id,
                        "discovery_source": "subcategory_bestseller",
                        "priority": priority,
                        "notes": f"shortlist_score={shortlist_score}",
                    }
                    for asin in asins
                ]

                new_count = self.storage.register_asins(discovered_rows)
                self.storage.mark_category_bestseller_done(category_id, self.domain, len(asins))

                raw_path = _save_raw_response(
                    raw_payload,
                    category="bestsellers",
                    label=f"subcat_{category_id}",
                    domain=self.domain,
                    asins=asins,
                )
                if raw_path:
                    self.storage.upsert_asin_raw_file_mappings(
                        asins=asins,
                        domain=self.domain,
                        source="subcategory_bestseller",
                        raw_file_path=raw_path,
                    )

                self.storage.record_discovery_expansion(
                    expansion_type="category",
                    domain=self.domain,
                    target_key=str(category_id),
                    target_label=str(category_name),
                    priority_score=shortlist_score,
                    candidate_count=requested,
                    new_asin_count=new_count,
                    notes=(
                        f"depth={candidate.get('category_depth')};sample_asins={candidate.get('sample_asin_count')};"
                        f"trend={candidate.get('trend_index_30d')}"
                    ),
                )

                self._stats["strategy_categories_selected"] += 1
                self._stats["strategy_asins_discovered"] += new_count
                logger.info(
                    f"L2/L3/L4 扩张: {category_id} ({category_name}) -> top {requested}, 新增 {new_count}, score={shortlist_score}"
                )
            except Exception as e:
                error_message = str(e)
                logger.warning(f"L2/L3/L4 扩张失败: {category_id} ({category_name}) -> {e}")
                self._stats["errors"].append(f"subcategory_expand_{category_id}: {e}")

            try:
                token_after = self.collector.check_token_status().get("tokens_left", tokens_before)
            except Exception:
                token_after = tokens_before

            try:
                self.storage.log_collection(
                    source="strategy_category_expand",
                    domain=self.domain,
                    asins_requested=requested,
                    asins_succeeded=new_count,
                    rows_ingested=0,
                    tokens_before=tokens_before,
                    tokens_after=token_after,
                    tokens_consumed=max(0, tokens_before - token_after),
                    duration_seconds=None,
                    raw_file_path=str(raw_path) if raw_path else None,
                    error_message=error_message,
                    started_at=started_at,
                )
            except Exception as e:
                logger.warning(f"写入 L2/L3/L4 扩张日志失败: {e}")

    def _expand_keywords(self) -> None:
        candidates = self.storage.get_keyword_expansion_candidates(
            domain=self.domain,
            limit=self.strategy_keyword_limit,
            cooldown_hours=self.strategy_keyword_cooldown_hours,
        )
        if not candidates:
            logger.info("未找到可扩张的 keyword")
            return

        logger.info(f"keyword 扩张候选 {len(candidates)} 个")
        for index, candidate in enumerate(candidates):
            try:
                token_info = self.collector.check_token_status()
                tokens_before = token_info.get("tokens_left", 0)
            except Exception:
                tokens_before = 0

            required_tokens = SEARCH_PRODUCTS_TOKENS_PER_PAGE + self.min_tokens_reserve
            if tokens_before < required_tokens:
                logger.info(f"token 不足 ({tokens_before})，跳过剩余 keyword 扩张")
                logger.info("本轮未执行的 keyword 候选不会写入 cooldown；后续 token 恢复后会重新参与扩张")
                try:
                    self.storage.log_collection(
                        source="strategy_keyword_skip",
                        domain=self.domain,
                        asins_requested=len(candidates) - index,
                        asins_succeeded=0,
                        rows_ingested=0,
                        tokens_before=tokens_before,
                        tokens_after=tokens_before,
                        tokens_consumed=0,
                        duration_seconds=None,
                        raw_file_path=None,
                        error_message="token_insufficient",
                        started_at=_utc_now_str(),
                    )
                except Exception as e:
                    logger.warning(f"写入 keyword token skip 日志失败: {e}")
                break

            keyword = str(candidate["keyword"])
            scope_decision = evaluate_seller_scope(query=keyword, keywords=[keyword])
            if not scope_decision.allowed:
                logger.info(
                    f"跳过超出中小跨境卖家经营范围的 keyword '{keyword}': "
                    f"{scope_decision.reason_code} {list(scope_decision.matched_terms)}"
                )
                self.storage.record_discovery_expansion(
                    expansion_type="keyword",
                    domain=self.domain,
                    target_key=keyword,
                    target_label=keyword,
                    priority_score=0,
                    candidate_count=0,
                    new_asin_count=0,
                    notes=(
                        f"seller_scope_blocked={scope_decision.reason_code};"
                        f"matched_terms={','.join(scope_decision.matched_terms)}"
                    ),
                )
                continue
            expand_priority = float(candidate.get("expand_priority") or 0)
            started_at = _utc_now_str()
            requested = 0
            new_count = 0
            error_message = None

            try:
                asins = self.discovery.search_products(term=keyword, domain=self.domain)
                requested = len(asins)
                priority = self._strategy_priority(70, expand_priority)
                discovered_rows = [
                    {
                        "asin": asin,
                        "domain": self.domain,
                        "search_term": keyword,
                        "discovery_source": "strategy_search",
                        "priority": priority,
                        "notes": f"keyword_expand_priority={expand_priority}",
                    }
                    for asin in asins
                ]
                new_count = self.storage.register_asins(discovered_rows)

                self.storage.record_discovery_expansion(
                    expansion_type="keyword",
                    domain=self.domain,
                    target_key=keyword,
                    target_label=keyword,
                    priority_score=expand_priority,
                    candidate_count=requested,
                    new_asin_count=new_count,
                    notes=(
                        f"trend_30d={candidate.get('trend_30d_avg')};growth_7d={candidate.get('trend_growth_7d')};"
                        f"mapped_asins={candidate.get('mapped_asin_count')}"
                    ),
                )

                self._stats["strategy_keywords_selected"] += 1
                self._stats["strategy_asins_discovered"] += new_count
                logger.info(
                    f"keyword 扩张: '{keyword}' -> 返回 {requested}, 新增 {new_count}, priority={expand_priority}"
                )
            except Exception as e:
                error_message = str(e)
                logger.warning(f"keyword 扩张失败: '{keyword}' -> {e}")
                self._stats["errors"].append(f"keyword_expand_{keyword}: {e}")

            try:
                token_after = self.collector.check_token_status().get("tokens_left", tokens_before)
            except Exception:
                token_after = tokens_before

            try:
                self.storage.log_collection(
                    source="strategy_keyword_expand",
                    domain=self.domain,
                    asins_requested=requested,
                    asins_succeeded=new_count,
                    rows_ingested=0,
                    tokens_before=tokens_before,
                    tokens_after=token_after,
                    tokens_consumed=max(0, tokens_before - token_after),
                    duration_seconds=None,
                    raw_file_path=None,
                    error_message=error_message,
                    started_at=started_at,
                )
            except Exception as e:
                logger.warning(f"写入 keyword 扩张日志失败: {e}")

    # ------------------------------------------------------------------
    # Google Trends: 关键词队列 + token 等待期间采集
    # ------------------------------------------------------------------

    def _refill_trends_queue(self) -> None:
        """从 DuckDB 注册表提取关键词, 补充待采集队列."""
        if self._trends_keyword_queue:
            return  # 队列还没消化完, 不补充

        rows = self.storage.conn.execute(
            """SELECT asin, product_title
               FROM curated.keepa_asin_registry
               WHERE product_title IS NOT NULL
                 AND domain = ?
                 AND is_active = TRUE
               ORDER BY last_fetched_at DESC NULLS LAST
               LIMIT 200""",
            [self.domain],
        ).fetchall()

        all_keywords: set[str] = set()
        mappings: list[tuple[str, int, list[str]]] = []
        for asin, title in rows:
            kws = extract_keywords_from_title(title, max_keywords=2)
            # 过滤: 至少 2 个词 且 <= 50 字符 (Google Trends 安全阈值)
            kws = [kw for kw in kws if len(kw.split()) >= 2 and len(kw) <= 50]
            if kws:
                all_keywords.update(kws)
                mappings.append((asin, self.domain, kws))

        # 持久化 asin↔keyword 映射
        if mappings:
            self.storage.upsert_asin_keywords_batch(mappings)

        # 排除本轮已采集的关键词
        new_keywords = sorted(all_keywords - self._trends_fetched_keywords)
        if new_keywords:
            self._trends_keyword_queue = new_keywords
            logger.info(f"Trends 队列补充 {len(new_keywords)} 个关键词 (已采集 {len(self._trends_fetched_keywords)} 个)")

    # Domain → pytrends hl 参数映射 (hl[-2:] 被 pytrends 用作 cookie 请求的 geo)
    _DOMAIN_TO_HL: dict[int, str] = {
        1:  "en-US",
        2:  "en-GB",
        3:  "de-DE",
        4:  "fr-FR",
        5:  "ja-JP",
        6:  "en-CA",
        8:  "it-IT",
        9:  "es-ES",
        10: "en-IN",
        11: "es-MX",
        12: "pt-BR",
        13: "en-AU",
    }

    def _get_trends_collector(self):
        """Lazy init GoogleTrendsCollector (按 domain 设置正确的 hl)."""
        if self._trends_collector is None:
            try:
                self._trends_collector = self._create_trends_collector()
            except ImportError:
                logger.warning("GoogleTrendsCollector 不可用 (pytrends 未安装)")
            except Exception as e:
                self._trends_collector = None
                reason = str(e).splitlines()[0][:180]
                switched = self._rotate_google_trends_proxy_node()
                if switched:
                    try:
                        self._trends_collector = self._create_trends_collector()
                        logger.info("GoogleTrendsCollector 切换 Mihomo 节点后初始化成功")
                        return self._trends_collector
                    except Exception as retry_error:
                        self._trends_collector = None
                        reason = str(retry_error).splitlines()[0][:180]
                self._pause_google_trends(reason)
                logger.warning(
                    "GoogleTrendsCollector 初始化失败，%s，已进入冷却: %s",
                    "已尝试切换 Mihomo 节点" if switched else "未切换节点",
                    reason,
                )
        return self._trends_collector

    def _create_trends_collector(self):
        from .collectors import GoogleTrendsCollector

        hl = self._DOMAIN_TO_HL.get(self.domain, "en-US")
        proxy_url = (
            os.environ.get("GOOGLE_TRENDS_PROXY_URL")
            or os.environ.get("AUTO_COLLECT_GOOGLE_TRENDS_PROXY_URL")
            or ""
        ).strip()
        if proxy_url:
            logger.info("Google Trends 使用显式代理: %s", self._redact_proxy_url(proxy_url))

        connect_timeout = self._env_float(
            "AUTO_GOOGLE_TRENDS_CONNECT_TIMEOUT_SECONDS",
            self._env_float("GOOGLE_TRENDS_CONNECT_TIMEOUT_SECONDS", 5.0),
        )
        read_timeout = self._env_float(
            "AUTO_GOOGLE_TRENDS_READ_TIMEOUT_SECONDS",
            self._env_float("GOOGLE_TRENDS_READ_TIMEOUT_SECONDS", 20.0),
        )
        retries = self._env_int("AUTO_GOOGLE_TRENDS_RETRIES", self._env_int("GOOGLE_TRENDS_RETRIES", 0))
        backoff_factor = self._env_float(
            "AUTO_GOOGLE_TRENDS_BACKOFF_FACTOR",
            self._env_float("GOOGLE_TRENDS_BACKOFF_FACTOR", 0.0),
        )
        return GoogleTrendsCollector(
            hl=hl,
            proxy_url=proxy_url or None,
            timeout=(max(0.5, connect_timeout), max(1.0, read_timeout)),
            retries=max(0, retries),
            backoff_factor=max(0.0, backoff_factor),
        )

    @staticmethod
    def _env_float(name: str, default: float) -> float:
        try:
            return float(os.environ.get(name, "") or default)
        except ValueError:
            return default

    @staticmethod
    def _env_int(name: str, default: int) -> int:
        try:
            return int(os.environ.get(name, "") or default)
        except ValueError:
            return default

    @staticmethod
    def _redact_proxy_url(proxy_url: str) -> str:
        parsed = urlsplit(proxy_url)
        if not parsed.username and not parsed.password:
            return proxy_url
        hostname = parsed.hostname or ""
        port = f":{parsed.port}" if parsed.port else ""
        return urlunsplit((parsed.scheme, f"***:***@{hostname}{port}", parsed.path, parsed.query, parsed.fragment))

    @staticmethod
    def _trends_timeframe() -> str:
        """Google Trends 时间范围：近 3 个月.

        使用 ``"today 3-m"`` 使 pytrends 返回 **日粒度** 数据。
        之前用 365 天自定义日期范围，pytrends 只返回周粒度数据，
        导致 keyword 扩张候选打分 (hot_days、trend_growth_7d)
        对非 US 站点全部失效（周数据在 7 天窗口内最多 1 个点）。
        候选打分仅依赖近 30 天窗口，90 天日粒度完全够用。
        """
        return "today 3-m"

    def _google_trends_pause_remaining_seconds(self) -> float:
        try:
            text = self.google_trends_cooldown_state_file.read_text(encoding="utf-8")
        except FileNotFoundError:
            return 0.0
        except Exception as e:
            logger.warning("读取 Google Trends 冷却状态失败: %s", e)
            return 0.0

        pause_until = 0.0
        for line in text.splitlines():
            if line.startswith("pause_until_epoch="):
                try:
                    pause_until = float(line.split("=", 1)[1])
                except ValueError:
                    pause_until = 0.0
                break
        return max(0.0, pause_until - time.time())

    def _google_trends_is_paused(self) -> bool:
        remaining = self._google_trends_pause_remaining_seconds()
        if remaining <= 0:
            return False
        logger.info("Google Trends 处于冷却中，剩余 %.0fs，本轮跳过", remaining)
        return True

    def _pause_google_trends(self, reason: str) -> None:
        if self.google_trends_cooldown_seconds <= 0:
            return
        pause_until = time.time() + self.google_trends_cooldown_seconds
        until_text = datetime.fromtimestamp(pause_until, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            self.google_trends_cooldown_state_file.parent.mkdir(parents=True, exist_ok=True)
            self.google_trends_cooldown_state_file.write_text(
                f"pause_until_epoch={pause_until}\nreason={reason}\npause_until_utc={until_text}\n",
                encoding="utf-8",
            )
            logger.info(
                "Google Trends 进入冷却 %ss，原因: %s，恢复时间(UTC): %s",
                self.google_trends_cooldown_seconds,
                reason,
                until_text,
            )
        except Exception as e:
            logger.warning("写入 Google Trends 冷却状态失败: %s", e)

    def _mihomo_request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        if not self.google_trends_mihomo_controller_url:
            return {}

        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers = {"Content-Type": "application/json"}
        if self.google_trends_mihomo_secret:
            headers["Authorization"] = f"Bearer {self.google_trends_mihomo_secret}"

        request = Request(
            f"{self.google_trends_mihomo_controller_url}{path}",
            data=data,
            headers=headers,
            method=method,
        )
        with urlopen(request, timeout=5) as response:
            body = response.read().decode("utf-8")
        return json.loads(body) if body else {}

    def _rotate_google_trends_proxy_node(self) -> bool:
        if not self.google_trends_mihomo_controller_url or not self.google_trends_mihomo_switch_group:
            return False

        try:
            proxies = self._mihomo_request("GET", "/proxies")
            proxy_map = proxies.get("proxies", {})
            group = proxy_map.get(self.google_trends_mihomo_switch_group) or {}
            candidates = [
                name
                for name in group.get("all", [])
                if name and name not in {"DIRECT", "REJECT"} and not (proxy_map.get(name) or {}).get("all")
            ]
            if len(candidates) < 2:
                logger.info(
                    "Google Trends Mihomo 分组 %s 可切换候选不足，跳过切节点",
                    self.google_trends_mihomo_switch_group,
                )
                return False

            current = group.get("now") or candidates[0]
            try:
                current_index = candidates.index(current)
            except ValueError:
                current_index = -1
            next_name = candidates[(current_index + 1) % len(candidates)]

            self._mihomo_request(
                "PUT",
                f"/proxies/{quote(self.google_trends_mihomo_switch_group, safe='')}",
                {"name": next_name},
            )
            self._trends_collector = None
            logger.info(
                "Google Trends Mihomo 分组 %s 已从 %s 切换到 %s",
                self.google_trends_mihomo_switch_group,
                current,
                next_name,
            )
            return True
        except Exception as e:
            logger.warning("Google Trends Mihomo 切节点失败: %s", e)
            return False

    def _handle_google_trends_error(self, batch: list[str], exc: Exception, *, prefix: str, batches_done: int) -> bool:
        should_pause = self._is_google_trends_rate_error(exc) or self._is_google_trends_transport_error(exc)
        if should_pause:
            self._trends_keyword_queue = batch + self._trends_keyword_queue
            reason = str(exc).splitlines()[0][:180]
            switched = self._rotate_google_trends_proxy_node()
            self._pause_google_trends(reason)
            logger.info(
                "%s Google Trends 限流/网络异常，%s，暂停 (已完成 %s 批): %s",
                prefix,
                "已尝试切换 Mihomo 节点" if switched else "未切换节点",
                batches_done,
                reason,
            )
            return True

        logger.warning("%s Google Trends %s 失败: %s", prefix, batch, exc)
        self._trends_fetched_keywords.update(batch)
        return False

    @staticmethod
    def _is_google_trends_rate_error(exc: Exception) -> bool:
        err_str = str(exc).lower()
        return any(marker in err_str for marker in ["429", "too many", "rate"])

    @staticmethod
    def _is_google_trends_transport_error(exc: Exception) -> bool:
        err_str = str(exc).lower()
        return any(
            marker in err_str
            for marker in [
                "network is unreachable",
                "timed out",
                "timeout",
                "proxyerror",
                "cannot connect to proxy",
                "ssl",
                "handshake",
                "unexpected eof",
                "connection reset",
            ]
        )

    def _fetch_trends_batch(self, *, batch: list[str], geo: str) -> list[dict]:
        collector = self._get_trends_collector()
        if collector is None:
            return []

        try:
            return collector.fetch_interest_over_time(
                keywords=batch,
                timeframe=self._trends_timeframe(),
                geo=geo,
            )
        except Exception as exc:
            if self._is_google_trends_rate_error(exc) or not self._is_google_trends_transport_error(exc):
                raise
            reason = str(exc).splitlines()[0][:180]
            switched = self._rotate_google_trends_proxy_node()
            if not switched:
                raise
            self._trends_collector = self._create_trends_collector()
            logger.info("Google Trends 网络异常，已切换 Mihomo 节点后重试: %s", reason)
            return self._trends_collector.fetch_interest_over_time(
                keywords=batch,
                timeframe=self._trends_timeframe(),
                geo=geo,
            )

    def _fetch_trends_during_wait(self, wait_secs: float) -> None:
        """在 token 等待期间采集 Google Trends (免费, 不消耗 Keepa token).

        在 wait_secs 时间预算内尽量多地从关键词队列消化关键词,
        每批 5 个关键词, pytrends 限流时提前退出.
        """
        if not self.enable_google_trends:
            time.sleep(wait_secs)
            return

        if self._google_trends_is_paused():
            time.sleep(wait_secs)
            return

        collector = self._get_trends_collector()
        if collector is None:
            time.sleep(wait_secs)
            return

        # 补充队列
        self._refill_trends_queue()
        if not self._trends_keyword_queue:
            time.sleep(wait_secs)
            return

        geo = KEEPA_DOMAIN_TO_GEO.get(self.domain, "")
        deadline = time.time() + wait_secs
        batches_done = 0

        while self._trends_keyword_queue and time.time() < deadline:
            if self.google_trends_max_batches_per_run and batches_done >= self.google_trends_max_batches_per_run:
                logger.info(
                    "  [等待期] Google Trends 本轮已处理 %s/%s 批，停止以降低频率",
                    batches_done,
                    self.google_trends_max_batches_per_run,
                )
                break

            batch = self._trends_keyword_queue[: self.google_trends_batch_size]
            self._trends_keyword_queue = self._trends_keyword_queue[self.google_trends_batch_size :]

            try:
                trend_rows = self._fetch_trends_batch(batch=batch, geo=geo)
                if trend_rows:
                    ingested = self.storage.ingest_google_trends(trend_rows)
                    self._stats["trends_rows_ingested"] += ingested
                    logger.info(f"  [等待期] Google Trends: {batch} → {ingested} 行")
                self._trends_fetched_keywords.update(batch)
                batches_done += 1
                if self.google_trends_request_interval_seconds > 0 and self._trends_keyword_queue:
                    time.sleep(min(self.google_trends_request_interval_seconds, max(0.0, deadline - time.time())))
            except Exception as e:
                should_break = self._handle_google_trends_error(
                    batch,
                    e,
                    prefix="  [等待期]",
                    batches_done=batches_done,
                )
                if should_break:
                    break

        # 剩余时间继续等待 (确保 token 有时间恢复)
        remaining = deadline - time.time()
        if remaining > 0:
            time.sleep(remaining)

    # ------------------------------------------------------------------
    # Phase 3: Google Trends (兜底, 处理等待期间遗漏的关键词)
    # ------------------------------------------------------------------

    def _fetch_google_trends(self) -> None:
        """从已采集的商品标题中提取关键词, 查询 Google Trends."""
        logger.info("--- Phase 3: Google Trends ---")

        if self._google_trends_is_paused():
            return

        # 确保 collector 使用正确的 hl (匹配当前 domain)

        # 从 DuckDB 中取已有商品标题
        rows = self.storage.conn.execute(
            """SELECT asin, product_title
               FROM curated.keepa_asin_registry
               WHERE product_title IS NOT NULL
                 AND domain = ?
               LIMIT 100""",
            [self.domain],
        ).fetchall()

        if not rows:
            logger.info("没有商品标题可提取关键词, 跳过")
            return

        # 提取关键词
        all_keywords: set[str] = set()
        mappings: list[tuple[str, int, list[str]]] = []
        for asin, title in rows:
            kws = extract_keywords_from_title(title, max_keywords=2)
            # 过滤: 至少 2 个词 且 <= 50 字符 (Google Trends 安全阈值)
            kws = [kw for kw in kws if len(kw.split()) >= 2 and len(kw) <= 50]
            if kws:
                all_keywords.update(kws)
                mappings.append((asin, self.domain, kws))

        # 持久化 asin↔keyword 映射
        if mappings:
            self.storage.upsert_asin_keywords_batch(mappings)

        if not all_keywords:
            logger.info("未提取到有效关键词, 跳过")
            return

        logger.info(f"提取到 {len(all_keywords)} 个关键词: {list(all_keywords)[:10]}...")

        # 根据 domain 确定 Google Trends 的 geo 参数
        geo = KEEPA_DOMAIN_TO_GEO.get(self.domain, "")
        logger.info(f"Google Trends geo: {geo!r} (domain={self.domain})")

        # 排除等待期间已采集的关键词, 避免重复请求
        all_keywords -= self._trends_fetched_keywords
        if not all_keywords:
            logger.info("所有关键词已在等待期间采集完成, 跳过")
            return

        keywords_list = sorted(all_keywords)[: self.google_trends_max_keywords_per_run]

        try:
            trends_collector = self._get_trends_collector()
            if trends_collector is None:
                return
            batches_done = 0
            for i in range(0, len(keywords_list), self.google_trends_batch_size):
                if self.google_trends_max_batches_per_run and batches_done >= self.google_trends_max_batches_per_run:
                    logger.info(
                        "  Google Trends 本轮已处理 %s/%s 批，停止以降低频率",
                        batches_done,
                        self.google_trends_max_batches_per_run,
                    )
                    break

                batch = keywords_list[i : i + self.google_trends_batch_size]
                try:
                    trend_rows = self._fetch_trends_batch(batch=batch, geo=geo)
                    if trend_rows:
                        ingested = self.storage.ingest_google_trends(trend_rows)
                        self._stats["trends_rows_ingested"] += ingested
                        logger.info(f"  Google Trends: {batch} → {ingested} 行")
                    self._trends_fetched_keywords.update(batch)
                    batches_done += 1
                    if self.google_trends_request_interval_seconds > 0 and i + self.google_trends_batch_size < len(keywords_list):
                        time.sleep(self.google_trends_request_interval_seconds)
                except Exception as e:
                    should_break = self._handle_google_trends_error(
                        batch,
                        e,
                        prefix=" ",
                        batches_done=batches_done,
                    )
                    if should_break:
                        break
                    time.sleep(10)

        except Exception as e:
            logger.error(f"Google Trends 采集失败: {e}")
            self._stats["errors"].append(f"trends: {e}")


# ---------------------------------------------------------------------------
# Raw response saving
# ---------------------------------------------------------------------------


def _resolve_raw_dir() -> Path:
    raw_json_root = os.environ.get("XIAMIMATE_RAW_JSON_ROOT")
    if raw_json_root:
        return Path(raw_json_root).expanduser().resolve()

    products_dir = os.environ.get("XIAMIMATE_RAW_PRODUCTS_DIR")
    if products_dir:
        return Path(products_dir).expanduser().resolve().parent

    return (Path(__file__).resolve().parents[2] / "data_platform" / "storage" / "raw" / "json").resolve()


def _resolve_fallback_raw_dir() -> Path | None:
    fallback_root = os.environ.get("XIAMIMATE_RAW_JSON_FALLBACK_ROOT")
    if fallback_root:
        return Path(fallback_root).expanduser().resolve()

    return None


_RAW_DIR = _resolve_raw_dir()
_FALLBACK_RAW_DIR = _resolve_fallback_raw_dir()


def _write_raw_response_files(
    *,
    out_dir: Path,
    payload: dict | list,
    label: str,
    timestamp: str,
    compression: str,
    category: str,
    domain: int | None,
    asins: list[str] | None,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    if compression == "gzip":
        path = out_dir / f"{label}_{timestamp}.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as output_file:
            json.dump(payload, output_file, ensure_ascii=False, default=str)
    else:
        path = out_dir / f"{label}_{timestamp}.json"
        with open(path, "w", encoding="utf-8") as output_file:
            json.dump(payload, output_file, ensure_ascii=False, default=str)

    meta_path = out_dir / f"{label}_{timestamp}.meta.json"
    meta_payload = {
        "category": category,
        "label": label,
        "saved_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "domain": domain,
        "asin_count": len(asins or []),
        "asins": asins or [],
        "json_file": path.name,
        "compression": compression,
        "is_compressed": compression == "gzip",
    }
    with open(meta_path, "w", encoding="utf-8") as output_file:
        json.dump(meta_payload, output_file, ensure_ascii=False, indent=2)

    return path


def _save_raw_response(
    payload: dict | list,
    category: str,
    label: str,
    *,
    domain: int | None = None,
    asins: list[str] | None = None,
    compression: str = "none",
) -> Path | None:
    """保存原始 API 响应到本地 JSON 文件.

    文件路径:
    - 未压缩: data_platform/storage/raw/json/{category}/{label}_{timestamp}.json
    - gzip:   data_platform/storage/raw/json/{category}/{label}_{timestamp}.json.gz
    元数据路径: 同目录 {label}_{timestamp}.meta.json

    Returns
    -------
    Path or None
        保存路径, 失败返回 None.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    try:
        path = _write_raw_response_files(
            out_dir=_RAW_DIR / category,
            payload=payload,
            label=label,
            timestamp=ts,
            compression=compression,
            category=category,
            domain=domain,
            asins=asins,
        )
        logger.debug(f"保存原始响应: {path}")
        return path
    except Exception as primary_error:
        if _FALLBACK_RAW_DIR is None or _FALLBACK_RAW_DIR == _RAW_DIR:
            logger.warning(f"保存原始响应失败: {primary_error}")
            return None
        try:
            fallback_path = _write_raw_response_files(
                out_dir=_FALLBACK_RAW_DIR / category,
                payload=payload,
                label=label,
                timestamp=ts,
                compression=compression,
                category=category,
                domain=domain,
                asins=asins,
            )
            logger.warning(f"保存原始响应到主目录失败: {primary_error}; 已写入 fallback: {fallback_path}")
            return fallback_path
        except Exception as fallback_error:
            logger.warning(f"保存原始响应失败: primary={primary_error}; fallback={fallback_error}")
        return None


# ---------------------------------------------------------------------------
# CLI 入口 helpers
# ---------------------------------------------------------------------------

def run_auto_collect(
    *,
    keepa_api_key: str,
    keepa_base_url: str = "https://api.keepa.com/product",
    db_path: str | Path | None = None,
    domain: int = 1,
    categories: list[int] | None = None,
    search_terms: list[str] | None = None,
    seed_file: str | Path | None = None,
    enable_google_trends: bool = False,
    enable_strategy_expansion: bool = False,
    strategy_pending_threshold: int = 200,
    strategy_category_limit: int = 2,
    strategy_keyword_limit: int = 5,
    strategy_category_cooldown_hours: int = 24 * 30,
    strategy_keyword_cooldown_hours: int = 72,
    stale_hours: int = 1440,
    batch_size: int = 50,
) -> dict[str, Any]:
    """启动一次自动采集, 供 CLI 调用."""

    with _duckdb_access_lock():
        collector = AutoCollector(
            keepa_api_key=keepa_api_key,
            keepa_base_url=keepa_base_url,
            db_path=db_path,
            domain=domain,
            categories=categories,
            search_terms=search_terms,
            seed_file=seed_file,
            enable_google_trends=enable_google_trends,
            enable_strategy_expansion=enable_strategy_expansion,
            strategy_pending_threshold=strategy_pending_threshold,
            strategy_category_limit=strategy_category_limit,
            strategy_keyword_limit=strategy_keyword_limit,
            strategy_category_cooldown_hours=strategy_category_cooldown_hours,
            strategy_keyword_cooldown_hours=strategy_keyword_cooldown_hours,
            stale_hours=stale_hours,
            batch_size=batch_size,
        )

        try:
            return collector.run()
        finally:
            collector.storage.close()


def run_auto_collect_loop(
    *,
    interval_minutes: int = 3,
    keepa_api_key: str,
    keepa_base_url: str = "https://api.keepa.com/product",
    db_path: str | Path | None = None,
    domain: int = 1,
    categories: list[int] | None = None,
    search_terms: list[str] | None = None,
    seed_file: str | Path | None = None,
    enable_google_trends: bool = False,
    enable_strategy_expansion: bool = False,
    strategy_pending_threshold: int = 200,
    strategy_category_limit: int = 2,
    strategy_keyword_limit: int = 5,
    strategy_category_cooldown_hours: int = 24 * 30,
    strategy_keyword_cooldown_hours: int = 72,
    stale_hours: int = 1440,
    batch_size: int = 50,
) -> None:
    """持续循环: 每隔 interval_minutes 运行一次 auto-collect.

    Token 策略 (21 token/min 套餐):
    - 桶容量 1260 token
    - Phase 2 (历史采集) 内部有智能等待: token 不足时等待恢复, 不退出
    - 一轮结束 = 所有 pending ASIN 采完, 等 interval_minutes 后检查新 BestSeller
    - Ctrl-C 优雅退出
    """
    round_num = 0
    total_asins = 0
    total_tokens = 0

    logger.info(
        f"=== 进入持续采集模式 === 轮间等待 {interval_minutes} 分钟, Ctrl-C 退出"
    )
    _install_shutdown_handler()

    collect_kwargs = dict(
        keepa_api_key=keepa_api_key,
        keepa_base_url=keepa_base_url,
        db_path=db_path,
        domain=domain,
        categories=categories,
        search_terms=search_terms,
        seed_file=seed_file,
        enable_google_trends=enable_google_trends,
        enable_strategy_expansion=enable_strategy_expansion,
        strategy_pending_threshold=strategy_pending_threshold,
        strategy_category_limit=strategy_category_limit,
        strategy_keyword_limit=strategy_keyword_limit,
        strategy_category_cooldown_hours=strategy_category_cooldown_hours,
        strategy_keyword_cooldown_hours=strategy_keyword_cooldown_hours,
        stale_hours=stale_hours,
        batch_size=batch_size,
    )

    try:
        while True:
            _raise_if_shutdown_requested()
            round_num += 1
            logger.info(f"--- 第 {round_num} 轮采集 ---")

            try:
                stats = run_auto_collect(**collect_kwargs)
                _raise_if_shutdown_requested()
                total_asins += stats.get("asins_fetched", 0)
                total_tokens += stats.get("tokens_consumed", 0)

                logger.info(
                    f"第 {round_num} 轮完成: "
                    f"本轮采集 {stats.get('asins_fetched', 0)} ASIN, "
                    f"消耗 {stats.get('tokens_consumed', 0)} token | "
                    f"累计 {total_asins} ASIN, {total_tokens} token"
                )
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                if _SHUTDOWN_REQUESTED:
                    raise KeyboardInterrupt
                logger.error(f"第 {round_num} 轮出错 (不退出, 等待下一轮): {exc}", exc_info=True)

            # 等待下一轮
            next_run = datetime.now(timezone.utc).strftime("%H:%M:%S")
            logger.info(
                f"等待 {interval_minutes} 分钟后开始第 {round_num + 1} 轮... "
                f"(当前 UTC {next_run})"
            )
            _sleep_until_shutdown_or_timeout(interval_minutes * 60)

    except KeyboardInterrupt:
        logger.info(
            f"\n=== 持续采集已停止 === "
            f"共 {round_num} 轮, 采集 {total_asins} ASIN, "
            f"消耗 {total_tokens} token"
        )


def run_multi_domain_collect_loop(
    *,
    domains: list[int] | None = None,
    interval_minutes: int = 3,
    keepa_api_key: str,
    keepa_base_url: str = "https://api.keepa.com/product",
    db_path: str | Path | None = None,
    seed_file: str | Path | None = None,
    enable_google_trends: bool = False,
    enable_strategy_expansion: bool = False,
    strategy_pending_threshold: int = 200,
    strategy_category_limit: int = 2,
    strategy_keyword_limit: int = 5,
    strategy_category_cooldown_hours: int = 24 * 30,
    strategy_keyword_cooldown_hours: int = 72,
    stale_hours: int = 1440,
    batch_size: int = 50,
) -> None:
    """多 domain 持续采集: 逐个 domain 完成所有 L1 类目的 top100 商品采集后再切换下一个.

    策略:
    - 外层: 遍历 domain 列表
    - 内层: 对当前 domain 循环执行 (发现 L1 类目 BestSeller → 采集 top100 历史数据),
      直到该 domain 所有 L1 类目均已采集完成 (bestseller_pending == 0 且 asins_pending == 0)
    - 一个 domain 的全部 L1 类目采完后才切换到下一个 domain
    - 所有 domain 完成一遍后等待 interval_minutes 再开始新一轮
    """
    target_domains = domains or ALL_DOMAINS
    geo_names = {d: KEEPA_DOMAIN_TO_GEO.get(d, "?") for d in target_domains}

    _install_shutdown_handler()

    logger.info(
        f"=== 多站点持续采集 (逐站点完成所有 L1) === {len(target_domains)} 个 domain: "
        + ", ".join(f"{d}/{geo_names[d]}" for d in target_domains)
        + f", 轮间等待 {interval_minutes} 分钟, Ctrl-C 退出"
    )

    round_num = 0
    total_asins = 0
    total_tokens = 0
    expansion_job_store = ExpansionJobStore()

    # 抢占公平阀：连续这么多次"补池抢占无进展"后，强制让正常跨域采集推进一轮，
    # 避免被 waiting_token / 无法 hydrate 的补池任务把整个多域循环永久锁死在单个 domain、
    # 饿死其他 domain 的正常采集。计数跨域累计，一旦有抢占取得进展即清零。
    preempt_stall_fairness_limit = 3
    preempt_stall_streak = 0

    collect_kwargs = dict(
        keepa_api_key=keepa_api_key,
        keepa_base_url=keepa_base_url,
        db_path=db_path,
        seed_file=seed_file,
        enable_google_trends=enable_google_trends,
        enable_strategy_expansion=enable_strategy_expansion,
        strategy_pending_threshold=strategy_pending_threshold,
        strategy_category_limit=strategy_category_limit,
        strategy_keyword_limit=strategy_keyword_limit,
        strategy_category_cooldown_hours=strategy_category_cooldown_hours,
        strategy_keyword_cooldown_hours=strategy_keyword_cooldown_hours,
        stale_hours=stale_hours,
        batch_size=batch_size,
    )

    try:
        while True:
            _raise_if_shutdown_requested()
            round_num += 1
            logger.info(f"--- 第 {round_num} 轮 (全站点) ---")

            # 周期性回收卡在 'discovering' 的孤儿补池任务（worker 中途崩溃留下的），让其可被重新领取，
            # 避免任务永久滞留、队列看似"一直有任务"却无法推进。
            try:
                requeued = expansion_job_store.requeue_stale_discovering_jobs(domains=target_domains)
                if requeued:
                    logger.warning(
                        f"  已重置 {requeued} 个卡住的 discovering 补池任务回 queued (worker 疑似中断)"
                    )
            except Exception as exc:
                logger.warning(f"重置卡住补池任务失败: {exc}")

            for domain in target_domains:
                _raise_if_shutdown_requested()
                geo = geo_names[domain]
                logger.info(
                    f"  >> Domain {domain}/{geo} 开始 (完成所有 L1 类目后再切换)"
                )
                domain_asins = 0
                domain_tokens = 0
                l1_round = 0

                while True:
                    _raise_if_shutdown_requested()
                    l1_round += 1

                    try:
                        preempt_domain = None
                        try:
                            preempt_domain = expansion_job_store.peek_next_interactive_job_domain(
                                domains=target_domains,
                            )
                        except Exception as exc:
                            logger.warning(f"读取全局补池抢占队列失败: {exc}")
                        if preempt_domain is not None and preempt_domain != domain:
                            if preempt_stall_streak >= preempt_stall_fairness_limit:
                                # 抢占公平阀：连续多次抢占都无进展（通常补池任务卡在 waiting_token /
                                # 无法 hydrate），本轮跳过抢占、让 Domain {domain} 的正常采集推进一次，
                                # 避免被无法推进的补池任务永久饿死跨域采集。随后清零，下一轮恢复抢占优先。
                                logger.warning(
                                    f"  !! 补池抢占连续 {preempt_stall_streak} 次无进展, "
                                    f"本轮让 Domain {domain}/{geo} 正常采集推进一次以避免跨域饥饿"
                                )
                                preempt_stall_streak = 0
                            else:
                                preempt_geo = geo_names.get(preempt_domain, KEEPA_DOMAIN_TO_GEO.get(preempt_domain, "?"))
                                logger.info(
                                    f"  !! 检测到 Domain {preempt_domain}/{preempt_geo} 交互式补池任务, "
                                    f"暂停 Domain {domain}/{geo} 普通采集并先处理补池"
                                )
                                preempt_stats = run_auto_collect(
                                    domain=preempt_domain,
                                    **collect_kwargs,
                                )
                                _raise_if_shutdown_requested()
                                preempt_fetched = preempt_stats.get("asins_fetched", 0)
                                preempt_discovered = preempt_stats.get("asins_discovered", 0)
                                preempt_consumed = preempt_stats.get("tokens_consumed", 0)
                                total_asins += preempt_fetched
                                total_tokens += preempt_consumed
                                logger.info(
                                    f"  !! Domain {preempt_domain}/{preempt_geo} 补池抢占轮完成: "
                                    f"发现 {preempt_discovered} ASIN, 采集 {preempt_fetched} ASIN, "
                                    f"消耗 {preempt_consumed} token"
                                )
                                if preempt_fetched == 0 and preempt_discovered == 0:
                                    preempt_stall_streak += 1
                                    logger.info(
                                        f"  !! Domain {preempt_domain}/{preempt_geo} 补池抢占轮暂无进展 "
                                        f"(连续 {preempt_stall_streak} 次), 等待 {interval_minutes} 分钟后重试"
                                    )
                                    _sleep_until_shutdown_or_timeout(interval_minutes * 60)
                                else:
                                    preempt_stall_streak = 0
                                continue

                        stats = run_auto_collect(
                            domain=domain,
                            **collect_kwargs,
                        )
                        _raise_if_shutdown_requested()
                        fetched = stats.get("asins_fetched", 0)
                        consumed = stats.get("tokens_consumed", 0)
                        domain_asins += fetched
                        domain_tokens += consumed
                        total_asins += fetched
                        total_tokens += consumed

                        bs_pending = stats.get("bestseller_pending", 0)
                        asin_pending = stats.get("asins_pending", 0)

                        logger.info(
                            f"  Domain {domain}/{geo} 第 {l1_round} 次: "
                            f"采集 {fetched} ASIN, 消耗 {consumed} token | "
                            f"待刷新类目 {bs_pending}, 剩余 ASIN {asin_pending}"
                        )

                        # 所有 L1 类目已采集且无待处理 ASIN → 该 domain 完成
                        if bs_pending == 0 and asin_pending == 0:
                            logger.info(
                                f"  Domain {domain}/{geo}: "
                                f"所有 L1 类目已完成, 切换下一站点"
                            )
                            break

                        # 安全阀: 本次既没发现新 ASIN 也没采集任何数据 → 避免死循环
                        if fetched == 0 and stats.get("asins_discovered", 0) == 0:
                            logger.info(
                                f"  Domain {domain}/{geo}: "
                                f"本次无新发现或采集 (可能 token 不足), "
                                f"等待 {interval_minutes} 分钟后重试"
                            )
                            _sleep_until_shutdown_or_timeout(interval_minutes * 60)

                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except Exception as exc:
                        if _SHUTDOWN_REQUESTED:
                            raise KeyboardInterrupt
                        logger.error(
                            f"  !! Domain {domain}/{geo} 第 {l1_round} 次出错: {exc}",
                            exc_info=True,
                        )
                        # 出错后等一会再重试, 不立刻切域
                        _sleep_until_shutdown_or_timeout(60)
                        # 连续出错多次则跳过该 domain
                        if l1_round >= 3 and domain_asins == 0:
                            logger.error(
                                f"  Domain {domain}/{geo}: 连续失败且无进展, 跳过"
                            )
                            break

                logger.info(
                    f"  << Domain {domain}/{geo} 完成: "
                    f"共采集 {domain_asins} ASIN, 消耗 {domain_tokens} token"
                )

            logger.info(
                f"第 {round_num} 轮完成 | 累计 {total_asins} ASIN, {total_tokens} token"
            )
            next_run = datetime.now(timezone.utc).strftime("%H:%M:%S")
            logger.info(
                f"等待 {interval_minutes} 分钟后开始第 {round_num + 1} 轮... "
                f"(当前 UTC {next_run})"
            )
            _sleep_until_shutdown_or_timeout(interval_minutes * 60)

    except (KeyboardInterrupt, SystemExit):
        logger.info(
            f"\n=== 多站点采集已停止 === "
            f"共 {round_num} 轮, 采集 {total_asins} ASIN, "
            f"消耗 {total_tokens} token"
        )
