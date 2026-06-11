from __future__ import annotations

from datetime import date, datetime, timezone
from io import StringIO
import csv
import warnings

import pandas as pd
from pytrends.request import TrendReq

from .base import BaseCollector, CollectorError
from ..utils import as_float, as_int, iso_date, utc_now_text

# pytrends 内部使用 df.fillna(False) 触发 FutureWarning (pandas >=2.1)
# 这是 pytrends 库的问题, 不影响功能, 直接抑制
warnings.filterwarnings(
    "ignore",
    message=".*Downcasting object dtype arrays.*",
    category=FutureWarning,
    module="pytrends",
)


class GoogleTrendsCollector:
    """Collector for Google Trends based on pytrends."""

    def __init__(
        self,
        hl: str = "en-US",
        tz: int = 0,
        proxy_url: str | None = None,
        timeout: tuple[float, float] = (5, 20),
        retries: int = 0,
        backoff_factor: float = 0.0,
    ) -> None:
        proxies = [proxy_url] if proxy_url else []
        self.client = TrendReq(
            hl=hl,
            tz=tz,
            proxies=proxies,
            timeout=timeout,
            retries=retries,
            backoff_factor=backoff_factor,
        )

    def fetch_interest_over_time(
        self,
        *,
        keywords: list[str],
        timeframe: str = "today 12-m",
        geo: str = "",
        category: int = 0,
        gprop: str = "",
    ) -> list[dict]:
        self.client.build_payload(keywords, cat=category, timeframe=timeframe, geo=geo, gprop=gprop)
        frame = self.client.interest_over_time()
        if frame.empty:
            raise CollectorError("Google Trends returned no rows for the given keywords.")

        frame = frame.reset_index()
        update_time = utc_now_text()
        rows: list[dict] = []
        for _, series in frame.iterrows():
            timestamp_value = series["date"]
            timestamp_text = timestamp_value.isoformat() if hasattr(timestamp_value, "isoformat") else str(timestamp_value)
            for keyword in keywords:
                rows.append(
                    {
                        "source": "Google Trends",
                        "country": geo or "GLOBAL",
                        "keyword_or_domain": keyword,
                        "data_type": "keyword",
                        "date": iso_date(series["date"]),
                        "trend_index": as_float(series.get(keyword)),
                        "search_volume": None,
                        "cpc": None,
                        "estimated_traffic": None,
                        "traffic_source": "Search Trend",
                        "backlinks": None,
                        "referring_domains": None,
                        "ranking_position": None,
                        "update_time": update_time,
                        "source_url": "https://trends.google.com",
                        "is_partial": bool(series.get("isPartial", False)),
                        "google_trends_timestamp": timestamp_text,
                    }
                )
        return rows


class SerpApiTrendsCollector(BaseCollector):
    """Google Trends 替代数据源兜底：通过 SerpApi 的 google_trends 引擎获取趋势。

    当主链路 pytrends 直连/代理持续 429 或网络异常时作为兜底，输出与
    ``GoogleTrendsCollector.fetch_interest_over_time`` 完全一致的行结构，
    以便复用同一套 ``DuckDBStorage.ingest_google_trends`` 写入逻辑。

    需要环境变量 ``SERPAPI_API_KEY``（或构造参数 ``api_key``）。未配置时
    调度侧不会实例化本采集器。
    """

    BASE_URL = "https://serpapi.com/search.json"

    def __init__(
        self,
        api_key: str,
        hl: str = "en-US",
        timeout: int = 30,
    ) -> None:
        super().__init__(timeout=timeout)
        self.api_key = (api_key or "").strip()
        self.hl = hl or "en-US"
        if not self.api_key:
            raise CollectorError("SerpApiTrendsCollector requires a non-empty api_key.")

    def fetch_interest_over_time(
        self,
        *,
        keywords: list[str],
        timeframe: str = "today 3-m",
        geo: str = "",
        category: int = 0,
        gprop: str = "",
    ) -> list[dict]:
        if not keywords:
            return []

        params: dict[str, str] = {
            "engine": "google_trends",
            "q": ",".join(keywords),
            "data_type": "TIMESERIES",
            "date": timeframe,
            "hl": self.hl,
            "api_key": self.api_key,
        }
        if geo:
            params["geo"] = geo
        if category:
            params["cat"] = str(category)
        if gprop:
            params["gprop"] = gprop

        payload = self.get_json(self.BASE_URL, params=params)
        if not isinstance(payload, dict):
            raise CollectorError("SerpApi returned an unexpected payload type.")
        if payload.get("error"):
            raise CollectorError("SerpApi error: %s" % payload.get("error"))

        interest = payload.get("interest_over_time") or {}
        timeline = interest.get("timeline_data") or []
        if not timeline:
            raise CollectorError("SerpApi returned no timeline_data for the given keywords.")

        update_time = utc_now_text()
        rows: list[dict] = []
        for point in timeline:
            if not isinstance(point, dict):
                continue
            point_date, timestamp_text = self._resolve_point_date(point)
            if point_date is None:
                continue
            values = point.get("values") or []
            value_by_query = {
                str(item.get("query")): item
                for item in values
                if isinstance(item, dict) and item.get("query") is not None
            }
            for index, keyword in enumerate(keywords):
                item = value_by_query.get(keyword)
                if item is None and index < len(values) and isinstance(values[index], dict):
                    item = values[index]
                trend_index = None
                is_partial = False
                if isinstance(item, dict):
                    trend_index = as_float(
                        item.get("extracted_value")
                        if item.get("extracted_value") is not None
                        else item.get("value")
                    )
                    is_partial = bool(item.get("partial_data") or item.get("is_partial"))
                rows.append(
                    {
                        "source": "SerpApi Google Trends",
                        "country": geo or "GLOBAL",
                        "keyword_or_domain": keyword,
                        "data_type": "keyword",
                        "date": iso_date(point_date),
                        "trend_index": trend_index,
                        "search_volume": None,
                        "cpc": None,
                        "estimated_traffic": None,
                        "traffic_source": "Search Trend",
                        "backlinks": None,
                        "referring_domains": None,
                        "ranking_position": None,
                        "update_time": update_time,
                        "source_url": "https://trends.google.com",
                        "is_partial": is_partial,
                        "google_trends_timestamp": timestamp_text,
                    }
                )
        if not rows:
            raise CollectorError("SerpApi timeline_data produced no usable rows.")
        return rows

    @staticmethod
    def _resolve_point_date(point: dict) -> tuple[date | None, str]:
        raw_ts = point.get("timestamp")
        if raw_ts is not None:
            try:
                dt = datetime.fromtimestamp(int(raw_ts), tz=timezone.utc)
                return dt.date(), dt.isoformat()
            except (TypeError, ValueError, OverflowError, OSError):
                pass
        raw_date = point.get("date")
        if raw_date:
            # SerpApi 日粒度形如 "Mar 9, 2025"；周粒度形如 "Mar 9 – 15, 2025"。
            text = str(raw_date).split("–")[0].split("-")[0].strip().rstrip(",").strip()
            for fmt in ("%b %d, %Y", "%b %d %Y", "%Y-%m-%d"):
                try:
                    parsed = datetime.strptime(text, fmt).date()
                    return parsed, str(raw_date)
                except ValueError:
                    continue
        return None, str(point.get("date") or "")


class SemrushCollector(BaseCollector):
    """Collector for Semrush CSV-style API responses."""

    def __init__(self, base_url: str, api_key: str, timeout: int = 60) -> None:
        super().__init__(timeout=timeout)
        self.base_url = base_url
        self.api_key = api_key

    def fetch_domain_history(
        self,
        *,
        domain: str,
        database: str = "us",
        report_type: str = "domain_rank_history",
        display_limit: int = 120,
        export_columns: str = "Rk,Or,Ot,Oc,Ad,At,Ac,Dt",
        display_daily: bool = False,
    ) -> list[dict]:
        params = {
            "type": report_type,
            "key": self.api_key,
            "domain": domain,
            "database": database,
            "display_limit": display_limit,
            "export_columns": export_columns,
            "export_decode": 1,
        }
        if display_daily:
            params["display_daily"] = 1

        raw_text = self.get_text(self.base_url, params=params)
        if raw_text.startswith("ERROR"):
            raise CollectorError(raw_text)

        reader = csv.DictReader(StringIO(raw_text), delimiter=";")
        update_time = utc_now_text()
        rows = []
        for row in reader:
            rows.append(
                {
                    "source": "Semrush",
                    "country": database.upper(),
                    "keyword_or_domain": domain,
                    "data_type": "domain",
                    "date": normalize_semrush_date(row.get("Date")),
                    "trend_index": None,
                    "search_volume": as_int(row.get("Organic Keywords")),
                    "cpc": None,
                    "estimated_traffic": as_int(row.get("Organic Traffic")),
                    "traffic_source": "Organic Search",
                    "backlinks": None,
                    "referring_domains": None,
                    "ranking_position": as_int(row.get("Rank")),
                    "update_time": update_time,
                    "source_url": self.base_url,
                    **{f"semrush_{key.lower().replace(' ', '_')}": value for key, value in row.items()},
                }
            )
        return rows


class AhrefsCollector(BaseCollector):
    """Collector for Ahrefs API v3.

    Because Ahrefs exposes many endpoint families and the exact response shape varies,
    this collector keeps the endpoint path configurable and applies a best-effort
    normalization for domain overview data.
    """

    def __init__(self, base_url: str, api_key: str, timeout: int = 60) -> None:
        super().__init__(timeout=timeout)
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def fetch_site_overview(
        self,
        *,
        target: str,
        country: str = "us",
        mode: str = "subdomains",
        endpoint_path: str = "site-explorer/overview",
        extra_params: dict[str, str] | None = None,
    ) -> list[dict]:
        url = f"{self.base_url}/{endpoint_path.lstrip('/')}"
        params = {
            "target": target,
            "country": country,
            "mode": mode,
        }
        if extra_params:
            params.update(extra_params)

        headers = {"Authorization": f"Bearer {self.api_key}"}
        payload = self.get_json(url, params=params, headers=headers)
        records = normalize_ahrefs_payload(payload)
        if not records:
            raise CollectorError("Ahrefs API returned no usable records.")

        today = date.today().isoformat()
        update_time = utc_now_text()
        rows = []
        for record in records:
            rows.append(
                {
                    "source": "Ahrefs",
                    "country": country.upper(),
                    "keyword_or_domain": target,
                    "data_type": "domain",
                    "date": today,
                    "trend_index": None,
                    "search_volume": as_int(record.get("search_volume") or record.get("organic_keywords")),
                    "cpc": as_float(record.get("cpc")),
                    "estimated_traffic": as_int(record.get("organic_traffic") or record.get("estimated_traffic") or record.get("traffic")),
                    "traffic_source": "Organic Search",
                    "backlinks": as_int(record.get("backlinks") or record.get("backlinks_count")),
                    "referring_domains": as_int(record.get("refdomains") or record.get("referring_domains")),
                    "ranking_position": as_int(record.get("position") or record.get("ranking_position")),
                    "update_time": update_time,
                    "source_url": url,
                    **{f"ahrefs_{key}": value for key, value in record.items()},
                }
            )
        return rows


def normalize_semrush_date(value: str | None) -> str:
    if not value:
        return date.today().isoformat()
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    return value


def normalize_ahrefs_payload(payload: object) -> list[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    for key in ("data", "rows", "results", "metrics", "overview"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            return [value]

    return [payload]
