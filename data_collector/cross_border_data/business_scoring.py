from __future__ import annotations

from bisect import bisect_left
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from .asin_discovery import extract_keywords_from_title
from .storage import DuckDBStorage

TIER_PRIORITY = {
    "P0": 95,
    "P1": 70,
    "Anchor": 55,
    "P2": 30,
    "Drop": 0,
}

SOURCE_PRIORITY_BONUS = {
    "manual": 10,
    "search": 8,
    "seed": 5,
    "bestseller": 0,
}

PRICE_BANDS_BY_DOMAIN: dict[int, tuple[float, float]] = {
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


def refresh_domain_business_priorities(
    storage: DuckDBStorage,
    *,
    domain: int = 1,
    asins: list[str] | None = None,
) -> dict[str, Any]:
    registry_rows = storage.conn.execute(
        """SELECT asin, domain, product_title, category, category_path, root_category_id,
                  discovery_source, search_term, priority, last_fetched_at, fetch_count
           FROM curated.keepa_asin_registry
           WHERE domain = ?
             AND is_active = TRUE""",
        [domain],
    ).fetchall()
    if not registry_rows:
        return {"scored": 0, "tiers": {}}

    history_30d_rows = storage.conn.execute(
        """SELECT asin, date, amazon_price, new_price, buy_box_price, list_price,
                  bsr, rating, review_count, monthly_sold, new_offer_count, used_offer_count
           FROM curated.keepa_product_history
           WHERE domain = ?
             AND date >= CURRENT_DATE - INTERVAL 30 DAY
           ORDER BY asin, date DESC""",
        [domain],
    ).fetchall()
    price_90d_rows = storage.conn.execute(
        """SELECT DISTINCT asin
           FROM curated.keepa_product_history
           WHERE domain = ?
             AND date >= CURRENT_DATE - INTERVAL 90 DAY
             AND (
               amazon_price IS NOT NULL OR new_price IS NOT NULL OR
               buy_box_price IS NOT NULL OR list_price IS NOT NULL
             )""",
        [domain],
    ).fetchall()
    snapshot_rows = storage.conn.execute(
        """SELECT asin,
                  COALESCE(total_offer_count, seller_count, retrieved_offer_count) AS current_offer_count
           FROM curated.keepa_product_snapshot
           WHERE domain = ?""",
        [domain],
    ).fetchall()

    target_asins = set(asins or [])
    price_90d_asins = {asin for asin, in price_90d_rows}
    snapshot_by_asin = {asin: offer_count for asin, offer_count in snapshot_rows}
    history_metrics = _build_history_metrics(history_30d_rows)
    registry_by_asin = {
        asin: {
            "asin": asin,
            "domain": row_domain,
            "product_title": product_title,
            "category": category,
            "category_path": category_path,
            "root_category_id": root_category_id,
            "discovery_source": discovery_source,
            "search_term": search_term,
            "priority": priority,
            "last_fetched_at": last_fetched_at,
            "fetch_count": fetch_count,
        }
        for asin, row_domain, product_title, category, category_path, root_category_id,
            discovery_source, search_term, priority, last_fetched_at, fetch_count in registry_rows
    }

    bsr_groups: dict[str, list[int]] = defaultdict(list)
    for asin, registry in registry_by_asin.items():
        latest_bsr = history_metrics.get(asin, {}).get("latest_bsr")
        if latest_bsr is None:
            continue
        key = _category_bucket(registry.get("root_category_id"))
        bsr_groups[key].append(int(latest_bsr))
    for values in bsr_groups.values():
        values.sort()

    score_rows: list[dict[str, Any]] = []
    tier_counter: Counter[str] = Counter()
    for asin, registry in registry_by_asin.items():
        if target_asins and asin not in target_asins:
            continue
        metrics = history_metrics.get(asin, {})
        result = _score_single_asin(
            registry=registry,
            metrics=metrics,
            price_90d_asins=price_90d_asins,
            snapshot_by_asin=snapshot_by_asin,
            bsr_groups=bsr_groups,
        )
        score_rows.append(
            {
                "asin": asin,
                "domain": domain,
                "business_score_total": result["business_score_total"],
                "business_tier": result["business_tier"],
                "business_priority": result["business_priority"],
            }
        )
        tier_counter[result["business_tier"]] += 1

    updated = storage.update_business_scores(score_rows)
    return {
        "scored": updated,
        "tiers": dict(sorted(tier_counter.items())),
    }


def _build_history_metrics(rows: list[tuple[Any, ...]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    for row in rows:
        grouped[str(row[0])].append(row)

    metrics: dict[str, dict[str, Any]] = {}
    for asin, asin_rows in grouped.items():
        latest_effective_price = None
        latest_bsr = None
        latest_rating = None
        latest_review_count = None
        latest_monthly_sold = None
        latest_new_offer_count = None
        new_offer_values: list[int] = []
        review_series: list[int] = []

        for _, _, amazon_price, new_price, buy_box_price, list_price, bsr, rating, review_count, monthly_sold, new_offer_count, _ in asin_rows:
            effective_price = _coalesce_price(buy_box_price, amazon_price, new_price, list_price)
            if latest_effective_price is None and effective_price is not None:
                latest_effective_price = effective_price
            if latest_bsr is None and bsr is not None:
                latest_bsr = int(bsr)
            if latest_rating is None and rating is not None:
                latest_rating = float(rating)
            if latest_review_count is None and review_count is not None:
                latest_review_count = int(review_count)
            if monthly_sold is not None and latest_monthly_sold is None:
                latest_monthly_sold = int(monthly_sold)
            if latest_new_offer_count is None and new_offer_count is not None:
                latest_new_offer_count = int(new_offer_count)
            if new_offer_count is not None:
                new_offer_values.append(int(new_offer_count))
            if review_count is not None:
                review_series.append(int(review_count))

        oldest_review_count = review_series[-1] if review_series else None
        review_delta_30d = None
        if latest_review_count is not None and oldest_review_count is not None:
            review_delta_30d = latest_review_count - oldest_review_count

        metrics[asin] = {
            "latest_effective_price": latest_effective_price,
            "latest_bsr": latest_bsr,
            "latest_rating": latest_rating,
            "latest_review_count": latest_review_count,
            "oldest_review_count": oldest_review_count,
            "review_delta_30d": review_delta_30d,
            "latest_monthly_sold": latest_monthly_sold,
            "latest_new_offer_count": latest_new_offer_count,
            "avg_new_offer_count": (
                sum(new_offer_values) / len(new_offer_values) if new_offer_values else None
            ),
        }
    return metrics


def _score_single_asin(
    *,
    registry: dict[str, Any],
    metrics: dict[str, Any],
    price_90d_asins: set[str],
    snapshot_by_asin: dict[str, Any],
    bsr_groups: dict[str, list[int]],
) -> dict[str, Any]:
    asin = str(registry["asin"])
    title = (registry.get("product_title") or "").strip()
    search_term = (registry.get("search_term") or "").strip()
    category_path = (registry.get("category_path") or "").strip()
    discovery_source = (registry.get("discovery_source") or "bestseller").strip().lower()
    root_category_id = registry.get("root_category_id")
    current_offer_count = snapshot_by_asin.get(asin)
    if current_offer_count is None:
        current_offer_count = metrics.get("avg_new_offer_count")
    if current_offer_count is None:
        current_offer_count = metrics.get("latest_new_offer_count")

    latest_bsr = metrics.get("latest_bsr")
    bsr_percentile = None
    if latest_bsr is not None:
        group = bsr_groups.get(_category_bucket(root_category_id), [])
        if group:
            bsr_percentile = _percent_rank(group, int(latest_bsr))

    keyword_candidates = extract_keywords_from_title(title, max_keywords=3) if title else []
    keyword_candidates = [kw for kw in keyword_candidates if len(kw) <= 50]

    hard_drop = False
    if not title and not search_term:
        hard_drop = True
    elif asin not in price_90d_asins and all(
        metrics.get(field) is None
        for field in ("latest_bsr", "latest_monthly_sold", "latest_review_count")
    ):
        hard_drop = True
    elif not category_path and registry.get("category") is None and root_category_id is None:
        hard_drop = True

    demand_score = _score_demand(metrics.get("latest_monthly_sold"), bsr_percentile)
    growth_score = _score_growth(
        metrics.get("latest_review_count"),
        metrics.get("latest_rating"),
        metrics.get("review_delta_30d"),
    )
    price_score = _score_price(registry.get("domain"), metrics.get("latest_effective_price"))
    competition_score = _score_competition(current_offer_count)
    category_score = _score_category(category_path, root_category_id)
    keyword_score = _score_keyword(keyword_candidates, search_term)

    business_score_total = sum(
        [
            demand_score,
            growth_score,
            price_score,
            competition_score,
            category_score,
            keyword_score,
        ]
    )

    latest_review_count = metrics.get("latest_review_count") or 0
    latest_monthly_sold = metrics.get("latest_monthly_sold") or 0
    anchor = (
        latest_review_count >= 5000 or
        latest_monthly_sold >= 1500 or
        (bsr_percentile is not None and bsr_percentile <= 0.05 and latest_review_count >= 2000)
    )

    if hard_drop:
        business_tier = "Drop"
    elif anchor:
        business_tier = "Anchor"
    elif business_score_total >= 9:
        business_tier = "P0"
    elif business_score_total >= 6:
        business_tier = "P1"
    else:
        business_tier = "P2"

    business_priority = _derive_business_priority(
        business_tier,
        discovery_source,
        registry.get("last_fetched_at"),
    )

    return {
        "business_score_total": int(business_score_total),
        "business_tier": business_tier,
        "business_priority": business_priority,
    }


def _score_demand(monthly_sold: int | None, bsr_percentile: float | None) -> int:
    if monthly_sold is not None and monthly_sold >= 200:
        return 2
    if bsr_percentile is not None and bsr_percentile <= 0.2:
        return 2
    if monthly_sold is not None and monthly_sold >= 50:
        return 1
    if bsr_percentile is not None and bsr_percentile <= 0.5:
        return 1
    return 0


def _score_growth(
    review_count: int | None,
    rating: float | None,
    review_delta_30d: int | None,
) -> int:
    if review_count is None:
        return 0
    if 50 <= review_count <= 1500 and (rating or 0) >= 4.0 and (review_delta_30d or 0) > 0:
        return 2
    if 20 <= review_count <= 5000 and (rating or 0) >= 3.7:
        return 1
    return 0


def _score_price(domain: int | None, effective_price: float | None) -> int:
    if effective_price is None:
        return 0
    low, high = PRICE_BANDS_BY_DOMAIN.get(int(domain or 1), PRICE_BANDS_BY_DOMAIN[1])
    if low <= effective_price <= high:
        return 2
    edge_low = low * (2.0 / 3.0)
    edge_high = high * (4.0 / 3.0)
    if edge_low <= effective_price <= edge_high:
        return 1
    return 0


def _score_competition(current_offer_count: float | int | None) -> int:
    if current_offer_count is None:
        return 0
    offer_count = float(current_offer_count)
    if offer_count <= 5:
        return 2
    if offer_count <= 10:
        return 1
    return 0


def _score_category(category_path: str, root_category_id: Any) -> int:
    if category_path:
        depth = category_path.count(" > ") + 1
        if depth >= 3:
            return 2
        if depth >= 2:
            return 1
    if root_category_id is not None:
        return 1
    return 0


def _score_keyword(keyword_candidates: list[str], search_term: str) -> int:
    if len(keyword_candidates) >= 2:
        return 2
    if keyword_candidates or search_term:
        return 1
    return 0


def _derive_business_priority(
    tier: str,
    discovery_source: str,
    last_fetched_at: Any,
) -> int:
    if tier == "Drop":
        return 0

    priority = TIER_PRIORITY.get(tier, 0)
    priority += SOURCE_PRIORITY_BONUS.get(discovery_source, 0)

    last_fetched = _as_datetime(last_fetched_at)
    now = datetime.now(timezone.utc)
    if last_fetched is None:
        priority += 10
    elif now - last_fetched >= timedelta(days=7):
        priority += 5

    return min(priority, 100)


def _category_bucket(root_category_id: Any) -> str:
    return str(root_category_id) if root_category_id is not None else "global"


def _percent_rank(sorted_values: list[int], value: int) -> float:
    if len(sorted_values) <= 1:
        return 0.0
    position = bisect_left(sorted_values, value)
    return position / max(len(sorted_values) - 1, 1)


def _coalesce_price(*values: Any) -> float | None:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _as_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            return None
    return None
