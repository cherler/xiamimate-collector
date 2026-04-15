from __future__ import annotations

MACRO_TRADE_FIELDS = [
    "source",
    "country",
    "partner_country",
    "hs_code",
    "hs_version",
    "product_desc",
    "trade_flow",
    "period",
    "trade_value",
    "quantity",
    "quantity_unit",
    "currency",
    "update_time",
    "source_url",
]

PRODUCT_TRACKING_FIELDS = [
    "source",
    "marketplace",
    "asin",
    "product_title",
    "brand",
    "category",
    "keyword",
    "date",
    "price",
    "list_price",
    "bsr",
    "rating",
    "review_count",
    "estimated_sales",
    "estimated_sales_period",
    "seller_count",
    "total_offer_count",
    "offer_count_fba",
    "offer_count_fbm",
    "retrieved_offer_count",
    "offers_successful",
    "stock_status",
    "data_capture_time",
    "source_url",
]

TRAFFIC_TREND_FIELDS = [
    "source",
    "country",
    "keyword_or_domain",
    "data_type",
    "date",
    "trend_index",
    "search_volume",
    "cpc",
    "estimated_traffic",
    "traffic_source",
    "backlinks",
    "referring_domains",
    "ranking_position",
    "update_time",
    "source_url",
]

PRODUCT_HISTORY_FIELDS = [
    "asin",
    "product_title",
    "brand",
    "category",
    "marketplace",
    "date",
    "amazon_price",
    "new_price",
    "used_price",
    "buy_box_price",
    "list_price",
    "bsr",
    "rating",
    "review_count",
    "monthly_sold",
    "new_offer_count",
    "used_offer_count",
]

FIELDNAMES_BY_TABLE = {
    "macro_trade_data": MACRO_TRADE_FIELDS,
    "product_tracking_data": PRODUCT_TRACKING_FIELDS,
    "product_history_data": PRODUCT_HISTORY_FIELDS,
    "traffic_trend_data": TRAFFIC_TREND_FIELDS,
}


def ordered_row(table_name: str, row: dict) -> dict:
    fieldnames = FIELDNAMES_BY_TABLE[table_name]
    normalized = {field: row.get(field) for field in fieldnames}
    extra_fields = [key for key in row.keys() if key not in normalized]
    for key in extra_fields:
        normalized[key] = row[key]
    return normalized
