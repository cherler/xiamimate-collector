from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re
from time import sleep

from .base import BaseCollector, CollectorError
from ..utils import as_float, as_int, read_csv_rows, utc_now_text


# Keepa Time epoch: keepaStartMinute = 21564000
# keepa_minute_to_unix_ms = (keepa_min + 21564000) * 60000
_KEEPA_START_MINUTE = 21_564_000


KEEPA_DOMAIN_LABELS = {
    1: "Amazon US",
    2: "Amazon UK",
    3: "Amazon DE",
    4: "Amazon FR",
    5: "Amazon JP",
    6: "Amazon CA",
    8: "Amazon IT",
    9: "Amazon ES",
    10: "Amazon IN",
    11: "Amazon MX",
    12: "Amazon BR",
    13: "Amazon AU",
}

# Keepa domain → Google Trends geo 映射
# Keepa domain 是 Amazon 站点编号, Google Trends geo 是 ISO 3166-1 alpha-2 国家代码
# 两者不是同一套编码! domain=1 → "US", domain=3 → "DE", 不等于 domain_id
KEEPA_DOMAIN_TO_GEO = {
    1:  "US",   # Amazon US  → United States
    2:  "GB",   # Amazon UK  → United Kingdom (注意: 不是 "UK")
    3:  "DE",   # Amazon DE  → Germany
    4:  "FR",   # Amazon FR  → France
    5:  "JP",   # Amazon JP  → Japan
    6:  "CA",   # Amazon CA  → Canada
    8:  "IT",   # Amazon IT  → Italy
    9:  "ES",   # Amazon ES  → Spain
    10: "IN",   # Amazon IN  → India
    11: "MX",   # Amazon MX  → Mexico
    12: "BR",   # Amazon BR  → Brazil
    13: "AU",   # Amazon AU  → Australia
}

SELLERSPRITE_ALIASES = {
    "asin": "asin",
    "ASIN": "asin",
    "商品ASIN": "asin",
    "父ASIN": "asin",
    "title": "product_title",
    "标题": "product_title",
    "商品标题": "product_title",
    "brand": "brand",
    "品牌": "brand",
    "category": "category",
    "类目": "category",
    "关键词": "keyword",
    "keyword": "keyword",
    "price": "price",
    "售价": "price",
    "价格": "price",
    "list_price": "list_price",
    "原价": "list_price",
    "BSR": "bsr",
    "排名": "bsr",
    "rating": "rating",
    "评分": "rating",
    "review_count": "review_count",
    "评论数": "review_count",
    "月销量": "estimated_sales",
    "预估销量": "estimated_sales",
}


class KeepaCollector(BaseCollector):
    """Collector for Keepa's product endpoint.

    Keepa's response contains many vendor-specific arrays. This implementation
    focuses on stable top-level fields and keeps the raw response optional.
    """

    # Keepa csv array indices (CsvType enum from Keepa Java API)
    CSV_AMAZON = 0
    CSV_NEW = 1
    CSV_USED = 2
    CSV_SALES = 3        # BSR / Sales Rank
    CSV_LISTPRICE = 4
    CSV_BUY_BOX = 18     # Buy Box price incl. shipping
    CSV_RATING = 16       # 0-50 scale (divide by 10)
    CSV_COUNT_REVIEWS = 17
    CSV_COUNT_NEW = 11
    CSV_COUNT_USED = 12

    # Domains where prices are in the smallest currency unit that is NOT cents
    # (i.e. Japan = yen, no division needed)
    _YEN_DOMAINS = {5}

    def __init__(self, base_url: str, api_key: str, timeout: int = 60) -> None:
        super().__init__(timeout=timeout)
        self.base_url = base_url
        self.api_key = api_key

    def fetch_products(
        self,
        *,
        asins: list[str],
        domain: int = 1,
        history: bool = True,
        stats_window_days: int = 90,
    ) -> tuple[list[dict], dict]:
        params = {
            "key": self.api_key,
            "domain": domain,
            "asin": ",".join(asins),
            "history": int(history),
            "stats": stats_window_days,
        }
        payload = self.get_json(self.base_url, params=params)
        products = payload.get("products", [])
        if not products:
            raise CollectorError("Keepa API returned no product rows.")

        update_time = utc_now_text()
        normalized = []
        for product in products:
            normalized.append(
                normalize_keepa_product_snapshot(
                    product,
                    domain=domain,
                    update_time=update_time,
                    source_url=self.base_url,
                )
            )
        return normalized, payload

    def fetch_product_history(
        self,
        *,
        asins: list[str],
        domain: int = 1,
        stats_window_days: int = 90,
    ) -> tuple[list[dict], dict]:
        """Fetch products with full history arrays and return flattened daily rows.

        Returns (history_rows, raw_payload) where each row is one date per ASIN
        containing: date, asin, amazon_price, new_price, used_price, buy_box_price,
        list_price, bsr, rating, review_count, monthly_sold, new_offer_count,
        used_offer_count.
        """
        params = {
            "key": self.api_key,
            "domain": domain,
            "asin": ",".join(asins),
            "history": 1,
            "stats": stats_window_days,
            "rating": 1,
        }
        payload = self.get_json(self.base_url, params=params)
        tokens_left = payload.get("tokensLeft")
        refill_in = payload.get("refillIn")
        products = payload.get("products", [])
        if not products:
            raise CollectorError("Keepa API returned no product rows.")

        is_yen = domain in self._YEN_DOMAINS
        all_rows: list[dict] = []

        for product in products:
            asin = product.get("asin")
            title = product.get("title")
            brand = product.get("brand")
            category = product.get("productGroup") or product.get("rootCategory")
            csv = product.get("csv") or []

            # 解析 categoryTree: [{catId: 123, name: "Home & Kitchen"}, ...]
            cat_tree = product.get("categoryTree")
            category_path = None
            root_category_id = None
            if cat_tree and isinstance(cat_tree, list) and len(cat_tree) > 0:
                names = [n.get("name", "") for n in cat_tree if n.get("name")]
                if names:
                    category_path = " > ".join(names)
                root_category_id = cat_tree[0].get("catId")

            # Parse each csv type into {date_str: value} dicts
            amazon_prices = _parse_keepa_csv_pair(csv, self.CSV_AMAZON, not is_yen)
            new_prices = _parse_keepa_csv_pair(csv, self.CSV_NEW, not is_yen)
            used_prices = _parse_keepa_csv_pair(csv, self.CSV_USED, not is_yen)
            buy_box_prices = _parse_keepa_csv_pair(csv, self.CSV_BUY_BOX, not is_yen)
            list_prices = _parse_keepa_csv_pair(csv, self.CSV_LISTPRICE, not is_yen)
            bsr_history = _parse_keepa_csv_pair(csv, self.CSV_SALES, False)
            rating_history = _parse_keepa_csv_pair(csv, self.CSV_RATING, False, scale=10)
            review_history = _parse_keepa_csv_pair(csv, self.CSV_COUNT_REVIEWS, False)
            new_count = _parse_keepa_csv_pair(csv, self.CSV_COUNT_NEW, False)
            used_count = _parse_keepa_csv_pair(csv, self.CSV_COUNT_USED, False)

            # monthlySoldHistory: [keepaTime, value, ...]
            monthly_sold_hist = _parse_keepa_timestamp_value_pairs(
                product.get("monthlySoldHistory")
            )

            # Collect all unique dates
            all_dates: set[str] = set()
            for d in (
                amazon_prices, new_prices, used_prices, buy_box_prices,
                list_prices, bsr_history, rating_history, review_history,
                new_count, used_count, monthly_sold_hist,
            ):
                all_dates.update(d.keys())

            for date_str in sorted(all_dates):
                row = {
                    "asin": asin,
                    "product_title": title,
                    "brand": brand,
                    "category": category,
                    "category_path": category_path,
                    "root_category_id": root_category_id,
                    "marketplace": KEEPA_DOMAIN_LABELS.get(domain, f"Amazon domain {domain}"),
                    "date": date_str,
                    "amazon_price": amazon_prices.get(date_str),
                    "new_price": new_prices.get(date_str),
                    "used_price": used_prices.get(date_str),
                    "buy_box_price": buy_box_prices.get(date_str),
                    "list_price": list_prices.get(date_str),
                    "bsr": bsr_history.get(date_str),
                    "rating": rating_history.get(date_str),
                    "review_count": review_history.get(date_str),
                    "monthly_sold": monthly_sold_hist.get(date_str),
                    "new_offer_count": new_count.get(date_str),
                    "used_offer_count": used_count.get(date_str),
                }
                all_rows.append(row)

        return all_rows, {
            "raw_products": payload,
            "tokens_left": tokens_left,
            "refill_in_ms": refill_in,
        }

    def check_token_status(self) -> dict:
        """Check remaining API tokens without consuming any."""
        payload = self.get_json(
            "https://api.keepa.com/token",
            params={"key": self.api_key},
        )
        return {
            "tokens_left": payload.get("tokensLeft"),
            "refill_in_ms": payload.get("refillIn"),
            "refill_rate": payload.get("refillRate"),
        }


class SellerSpriteImporter:
    """Normalize exported SellerSprite CSV files into the standard product table."""

    def import_file(self, input_path: str | Path, marketplace: str) -> list[dict]:
        rows = read_csv_rows(input_path)
        if not rows:
            raise CollectorError("SellerSprite export file is empty.")

        update_time = utc_now_text()
        normalized = []
        for row in rows:
            current = {
                "source": "SellerSprite",
                "marketplace": marketplace,
                "asin": None,
                "product_title": None,
                "brand": None,
                "category": None,
                "keyword": None,
                "date": update_time[:10],
                "price": None,
                "list_price": None,
                "bsr": None,
                "rating": None,
                "review_count": None,
                "estimated_sales": None,
                "estimated_sales_period": "monthly",
                "seller_count": None,
                "stock_status": None,
                "data_capture_time": update_time,
                "source_url": str(Path(input_path).resolve()),
            }
            extras = {}
            for key, value in row.items():
                target_key = SELLERSPRITE_ALIASES.get(key, SELLERSPRITE_ALIASES.get(key.strip()))
                if target_key:
                    current[target_key] = normalize_value(target_key, value)
                else:
                    extras[f"raw_{key}"] = value
            normalized.append({**current, **extras})
        return normalized


def normalize_value(field_name: str, value: str | None):
    if field_name in {"price", "list_price", "rating"}:
        return as_float(value)
    if field_name in {"bsr", "review_count", "estimated_sales", "seller_count"}:
        return as_int(value)
    return value


def keepa_price(product: dict) -> float | None:
    for key in ("buyBoxPrice", "amazonPrice", "newPrice"):
        price = as_float(product.get(key))
        if price is not None:
            return round(price / 100, 2) if price > 1000 else price

    # Keepa's raw price arrays are vendor-specific. This fallback avoids hard-coding
    # an unstable index map and keeps the code safe across API plans.
    return None


def keepa_rating(product: dict) -> float | None:
    rating = as_float(product.get("rating") or product.get("lastRatingUpdate"))
    if rating is None:
        return None
    return round(rating / 10, 2) if rating > 10 else rating


def _normalize_keepa_count(value: object) -> int | None:
    normalized = as_int(value)
    if normalized is None or normalized < 0:
        return None
    return normalized


def normalize_keepa_product_snapshot(
    product: dict,
    *,
    domain: int,
    update_time: str,
    source_url: str,
) -> dict:
    stats = product.get("stats") or {}
    total_offer_count = _normalize_keepa_count(stats.get("totalOfferCount"))

    return {
        "source": "Keepa",
        "marketplace": KEEPA_DOMAIN_LABELS.get(domain, f"Amazon domain {domain}"),
        "asin": product.get("asin"),
        "product_title": product.get("title"),
        "brand": product.get("brand"),
        "category": product.get("productGroup") or product.get("rootCategory"),
        "keyword": None,
        "date": update_time[:10],
        "price": keepa_price(product),
        "list_price": None,
        "bsr": None,
        "rating": keepa_rating(product),
        "review_count": as_int(product.get("reviews")),
        "estimated_sales": as_int(product.get("monthlySold")),
        "estimated_sales_period": "monthly" if product.get("monthlySold") else None,
        "seller_count": total_offer_count,
        "total_offer_count": total_offer_count,
        "offer_count_fba": _normalize_keepa_count(stats.get("offerCountFBA")),
        "offer_count_fbm": _normalize_keepa_count(stats.get("offerCountFBM")),
        "retrieved_offer_count": _normalize_keepa_count(stats.get("retrievedOfferCount")),
        "offers_successful": product.get("offersSuccessful"),
        "stock_status": None,
        "data_capture_time": update_time,
        "source_url": source_url,
        "keepa_domain": domain,
        "keepa_last_update": product.get("lastUpdate"),
    }


# ---------------------------------------------------------------------------
# Keepa time & csv history helpers
# ---------------------------------------------------------------------------

def keepa_minute_to_datetime(keepa_min: int) -> datetime:
    """Convert a Keepa Time minute value to a UTC datetime."""
    unix_ms = (keepa_min + _KEEPA_START_MINUTE) * 60_000
    return datetime.fromtimestamp(unix_ms / 1000, tz=timezone.utc)


def _parse_keepa_csv_pair(
    csv_2d: list,
    index: int,
    is_price: bool,
    *,
    scale: int = 0,
) -> dict[str, float | int | None]:
    """Parse a Keepa csv[index] array of [timestamp, value, ...] into {date_str: value}.

    - is_price=True: divide by 100 to convert cents → dollars (except yen).
    - scale>0: divide value by scale (e.g. rating is 0-50, scale=10 → 0.0-5.0).
    - value of -1 means out of stock / unavailable → stored as None.
    - When multiple entries fall on the same date, the last one wins.
    """
    result: dict[str, float | int | None] = {}
    if not csv_2d or index >= len(csv_2d):
        return result
    arr = csv_2d[index]
    if not arr:
        return result

    i = 0
    while i + 1 < len(arr):
        keepa_min = arr[i]
        raw_value = arr[i + 1]
        i += 2

        try:
            dt = keepa_minute_to_datetime(keepa_min)
        except (OSError, OverflowError, ValueError):
            continue

        date_str = dt.strftime("%Y-%m-%d")

        if raw_value is None or raw_value == -1:
            result[date_str] = None
            continue

        value = float(raw_value)
        if is_price:
            value = round(value / 100, 2)
        elif scale > 0:
            value = round(value / scale, 2)
        else:
            value = int(value) if value == int(value) else value

        result[date_str] = value

    return result


def _parse_keepa_timestamp_value_pairs(arr: list | None) -> dict[str, int | None]:
    """Parse [keepaTime, value, keepaTime, value, ...] into {date_str: value}."""
    result: dict[str, int | None] = {}
    if not arr:
        return result

    i = 0
    while i + 1 < len(arr):
        keepa_min = arr[i]
        raw_value = arr[i + 1]
        i += 2

        try:
            dt = keepa_minute_to_datetime(keepa_min)
        except (OSError, OverflowError, ValueError):
            continue

        date_str = dt.strftime("%Y-%m-%d")
        result[date_str] = int(raw_value) if raw_value is not None and raw_value != -1 else None

    return result

