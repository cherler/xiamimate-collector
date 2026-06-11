"""ASIN 发现 + 关键词提取.

提供四种 ASIN 获取方式:
1. Keepa Best Sellers API (免费, 0 token) — 获取品类下 Top ASIN 列表
2. Keepa Product Search API (按 10 token / 结果页预算) — 关键词搜索 ASIN
3. Keepa Product Finder API (/query, 约 10 token / 次) — 按价格/销量/排名等条件批量发现 ASIN
4. 种子文件 — 手工维护的 ASIN 列表 CSV

以及关键词提取:
- 从 Keepa 返回的商品标题中自动提取 Google Trends 搜索词
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path

from .collectors.base import BaseCollector, CollectorError
from .seller_scope import filter_seller_scope_keywords
from .utils import utc_now_text


SEARCH_PRODUCTS_TOKENS_PER_PAGE = 10
PRODUCT_FINDER_TOKENS_PER_QUERY = 10


# 用于过滤标题中的噪音词
_STOP_WORDS = {
    "a", "an", "the", "and", "or", "for", "with", "in", "on", "of", "to",
    "by", "from", "is", "at", "its", "it", "this", "that", "as", "be",
    "are", "was", "were", "been", "have", "has", "had", "do", "does", "did",
    "will", "would", "could", "should", "may", "might", "can", "must",
    "shall", "need", "not", "but", "all", "also", "very", "just", "only",
    "more", "most", "than", "like", "over", "into", "some", "any", "each",
    "other", "our", "your", "their", "about", "out", "you", "hot",
    # Amazon 特有噪音
    "amazon", "pack", "count", "set", "size", "color", "colour", "style",
    "edition", "version", "gen", "generation", "model", "series", "new",
    "latest", "updated", "upgrade", "upgraded",
    # 性别/人群修饰词 (Google Trends 搜 "women flip flops" 不如 "flip flops")
    "women", "womens", "woman", "men", "mens", "man",
    "kids", "kid", "boys", "boy", "girls", "girl",
    "baby", "babies", "toddler", "toddlers", "adult", "adults",
    "unisex", "teen", "teens",
    # 尺寸/度量修饰词
    "inch", "inches", "feet", "foot", "large", "small", "medium",
    "big", "mini", "slim", "fit", "fits", "fitted", "wide", "narrow",
    "long", "short", "tall", "extra", "plus",
    # 颜色词 (不适合做趋势搜索)
    "black", "white", "red", "blue", "green", "pink", "grey", "gray",
    "brown", "purple", "orange", "yellow", "gold", "silver",
    # 材质/通用描述 (太泛)
    "cotton", "leather", "plastic", "metal", "stainless", "steel",
    "rubber", "silicone", "nylon", "polyester",
    # 常见 Amazon 标题 filler
    "best", "top", "premium", "professional", "pro", "ultra", "super",
    "deluxe", "original", "classic", "basic", "essential", "everyday",
    "perfect", "ideal", "great", "good", "nice", "comfortable",
    "lightweight", "portable", "durable", "heavy", "duty",
    "multi", "purpose", "piece", "pcs", "pair", "pairs",
    "compatible", "universal", "adjustable", "waterproof", "resistant",
    "gift", "gifts", "christmas", "birthday", "holiday",
    # 季节
    "summer", "winter", "spring", "fall", "autumn",
    # Amazon 营销词
    "bestseller", "seller", "selling", "rated", "quality",
}

# 品牌词通常需要过滤掉 (不适合做 Google Trends 搜索)
_COMMON_BRANDS = {
    "apple", "samsung", "sony", "lg", "nike", "adidas", "anker", "baseus",
    "xiaomi", "huawei", "jbl", "bose", "logitech", "microsoft", "google",
    "amazon", "dell", "hp", "lenovo", "asus", "acer",
}


class KeepaAsinDiscovery:
    """通过 Keepa API 发现 ASIN."""

    def __init__(self, base_url: str, api_key: str, timeout: int = 120) -> None:
        self._collector = _SimpleKeepaClient(base_url, api_key, timeout)
        self.api_key = api_key

    def fetch_best_sellers(
        self,
        *,
        category: int | str,
        domain: int = 1,
    ) -> tuple[list[str], dict]:
        """获取品类下的 Best Seller ASIN 列表.

        消耗 50 token, 返回品类下完整 BestSeller 排行榜 (最多 ~100K ASIN).

        Parameters
        ----------
        category : int or str
            品类节点 ID 或 productGroup 名称.
        domain : int
            Amazon 站点 ID (1=US, 2=UK, ...).

        Returns
        -------
        tuple[list[str], dict]
            (ASIN 列表, 原始 API 响应 payload)
        """
        payload = self._collector.get_json(
            "https://api.keepa.com/bestsellers",
            params={
                "key": self.api_key,
                "domain": domain,
                "category": str(category),
            },
        )
        asins = payload.get("bestSellersList", {}).get("asinList") or []
        if not asins:
            # 某些品类返回格式不同
            asins = payload.get("asinList") or []
        return asins, payload

    def search_products(
        self,
        *,
        term: str,
        domain: int = 1,
        asins_only: bool = True,
    ) -> list[str] | list[dict]:
        """通过关键词搜索 ASIN.

        按 10 token / 结果页做预算.

        Keepa 文档将 product search 定义为每个结果页消耗 10 token。
        当前调度器与自动扩张逻辑均按单次 search 至少预留 10 token，
        避免低估搜索成本。

        Parameters
        ----------
        term : str
            搜索关键词.
        domain : int
            Amazon 站点 ID.
        asins_only : bool
            True=只返回 ASIN 列表, False=返回完整商品对象.

        Returns
        -------
        list[str] or list[dict]
        """
        params = {
            "key": self.api_key,
            "domain": domain,
            "type": "product",
            "term": term,
            "asins-only": "1" if asins_only else "0",
            "history": "0",
            "update": "24",  # 24小时内的缓存数据即可, 避免多消耗token
        }
        payload = self._collector.get_json(
            "https://api.keepa.com/search",
            params=params,
        )
        if asins_only:
            return payload.get("asinList") or []
        return payload.get("products") or []

    def find_products(
        self,
        *,
        selection: dict,
        domain: int = 1,
    ) -> tuple[list[str], dict]:
        """Keepa Product Finder (/query): 按筛选条件批量发现 ASIN.

        相比 BestSeller(只能按品类)和关键词搜索(只能按词), Product Finder
        可直接用价格区间 / 销量排名 / 月销量 / 品类等结构化条件圈出符合
        中小跨境卖家经营范围的 ASIN, 作为 BestSeller 枯竭 / 关键词扩张
        受限时的机制性发现源兜底.

        约 10 token / 次 (见 ``PRODUCT_FINDER_TOKENS_PER_QUERY``).

        Parameters
        ----------
        selection : dict
            Keepa Product Finder 过滤 JSON, 例如
            ``{"current_NEW_gte": 1500, "current_NEW_lte": 6000,
               "current_SALES_lte": 80000, "monthlySold_gte": 100,
               "sort": [["current_SALES", "asc"]], "perPage": 200, "page": 0}``。
        domain : int
            Amazon 站点 ID.

        Returns
        -------
        tuple[list[str], dict]
            (ASIN 列表, 原始 API 响应 payload)
        """
        payload = self._collector.get_json(
            "https://api.keepa.com/query",
            params={
                "key": self.api_key,
                "domain": domain,
                "selection": json.dumps(selection, separators=(",", ":")),
            },
        )
        asins = payload.get("asinList") or []
        return asins, payload

    def check_tokens(self) -> dict:
        """检查剩余 token."""
        payload = self._collector.get_json(
            "https://api.keepa.com/token",
            params={"key": self.api_key},
        )
        return {
            "tokens_left": payload.get("tokensLeft"),
            "refill_in_ms": payload.get("refillIn"),
            "refill_rate": payload.get("refillRate"),
        }


class _SimpleKeepaClient(BaseCollector):
    """轻量 HTTP 客户端, 复用 BaseCollector 的 session 管理."""

    def __init__(self, base_url: str, api_key: str, timeout: int = 60) -> None:
        super().__init__(timeout=timeout)


# ---------------------------------------------------------------------------
# 种子文件管理
# ---------------------------------------------------------------------------

def load_seed_asins(path: str | Path) -> list[dict]:
    """从种子文件加载 ASIN 列表.

    CSV 格式: asin, marketplace, category, keyword, priority, notes
    至少需要 asin 列.
    """
    path = Path(path).resolve()
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    return [r for r in rows if r.get("asin", "").strip()]


def save_discovered_asins(
    asins: list[dict],
    output_path: str | Path,
) -> Path:
    """保存发现的 ASIN 到 CSV."""
    path = Path(output_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "asin", "marketplace", "domain", "category_id", "category_name",
        "discovery_source", "search_term", "discovered_at", "priority", "notes",
    ]

    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(asins)
    return path


# ---------------------------------------------------------------------------
# 关键词提取
# ---------------------------------------------------------------------------

def extract_keywords_from_title(
    title: str,
    *,
    max_keywords: int = 3,
    min_word_length: int = 3,
    remove_brands: bool = True,
) -> list[str]:
    """从商品标题中提取 Google Trends 搜索关键词.

    策略:
    1. 去掉括号内容 (通常是规格描述)
    2. 去掉停用词和品牌词
    3. 提取连续的名词短语作为关键词
    4. 返回最多 max_keywords 个关键词

    Examples
    --------
    >>> extract_keywords_from_title(
    ...     "Apple iPad Air (5th Generation): with M1 chip, 10.9-inch "
    ...     "Liquid Retina Display, 64GB, Wi-Fi 6"
    ... )
    ['ipad air', 'liquid retina display']

    >>> extract_keywords_from_title(
    ...     "Insulated Lunch Bag for Women Men, Reusable Lunch Box"
    ... )
    ['insulated lunch bag', 'reusable lunch box']
    """
    if not title:
        return []

    # 1. 去掉括号/方括号内容 (规格、代号)
    cleaned = re.sub(r"\([^)]*\)", " ", title)
    cleaned = re.sub(r"\[[^\]]*\]", " ", cleaned)

    # 2. 逗号/斜杠/竖线/分号 → 短语分隔 (Amazon 标题常用这些分隔卖点)
    cleaned = re.sub(r"[,/|;]+", " , ", cleaned)

    # 3. 去掉特殊字符, 保留字母、数字、空格、连字符、逗号(分隔符)
    cleaned = re.sub(r"[^a-zA-Z0-9\s,\-]", " ", cleaned)

    # 3. 分词并过滤
    words = cleaned.lower().split()
    filtered_stop = set(_STOP_WORDS)
    if remove_brands:
        filtered_stop |= _COMMON_BRANDS

    # 4. 提取连续的有意义词组 (不被停用词/品牌/逗号打断的连续词)
    phrases: list[list[str]] = []
    current_phrase: list[str] = []

    for word in words:
        word = word.strip("-")

        # 逗号分隔符 → 强制断句
        if word == ",":
            if current_phrase:
                phrases.append(current_phrase)
                current_phrase = []
            continue

        if not word or len(word) < min_word_length:
            if current_phrase:
                phrases.append(current_phrase)
                current_phrase = []
            continue

        if word in filtered_stop:
            if current_phrase:
                phrases.append(current_phrase)
                current_phrase = []
            continue

        # 过滤纯数字 (64GB → "64" 没意义)
        if re.match(r"^\d+$", word):
            if current_phrase:
                phrases.append(current_phrase)
                current_phrase = []
            continue

        current_phrase.append(word)

    if current_phrase:
        phrases.append(current_phrase)

    # 5. 截断过长的词组 (Google Trends 对过长关键词返回 400)
    #    保留前 4 个词, 超过的拆分为新词组
    _MAX_PHRASE_WORDS = 4
    trimmed_phrases: list[list[str]] = []
    for phrase in phrases:
        while len(phrase) > _MAX_PHRASE_WORDS:
            trimmed_phrases.append(phrase[:_MAX_PHRASE_WORDS])
            phrase = phrase[_MAX_PHRASE_WORDS:]
        if phrase:
            trimmed_phrases.append(phrase)
    phrases = trimmed_phrases

    # 6. 合并词组
    keywords = [" ".join(phrase) for phrase in phrases if len(phrase) >= 1]

    # 过滤掉超过 50 字符的关键词 (Google Trends 安全阈值)
    keywords = [kw for kw in keywords if len(kw) <= 50]

    # 优先选择 2-4 词的短语
    multi_word = [kw for kw in keywords if len(kw.split()) >= 2]
    single_word = [kw for kw in keywords if len(kw.split()) == 1]

    result = multi_word[:max_keywords]
    remaining = max_keywords - len(result)
    if remaining > 0:
        result.extend(single_word[:remaining])

    result, _blocked = filter_seller_scope_keywords(result)
    return result


def extract_keywords_batch(
    products: list[dict],
    *,
    title_field: str = "product_title",
    max_keywords: int = 3,
) -> dict[str, list[str]]:
    """批量提取: {asin: [keyword1, keyword2, ...]}."""
    result: dict[str, list[str]] = {}
    for product in products:
        asin = product.get("asin", "")
        title = product.get(title_field, "")
        if asin and title:
            result[asin] = extract_keywords_from_title(title, max_keywords=max_keywords)
    return result
