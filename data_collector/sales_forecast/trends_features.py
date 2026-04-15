"""Google Trends 特征工程.

将小时级 trend_index 聚合为日级, 并计算变化量、平滑、波动、相对位置等衍生特征.

输出字段:
  原始热度:
    google_trends_index            — 日均值 (小时级 → 天级)
  变化量特征:
    google_trends_index_dod        — 日环比 (Day-over-Day)
    google_trends_index_wow        — 周同比 (Week-over-Week)
  平滑特征:
    google_trends_3d_mean          — 3 日移动平均
    google_trends_7d_mean          — 7 日移动平均
  波动特征:
    google_trends_7d_std           — 7 日滚动标准差
    google_trends_7d_max           — 7 日滚动最大值
  相对位置特征:
    google_trends_vs_7d_mean_ratio — 当日值 / 7日均值
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


def load_hourly_trends(files: Iterable[str | Path]) -> pd.DataFrame:
    """加载一个或多个 Google Trends 小时级 CSV 文件."""
    frames = []
    for f in files:
        p = Path(f).resolve()
        if p.exists():
            frames.append(pd.read_csv(p))
    if not frames:
        raise FileNotFoundError("未找到任何 Google Trends CSV 文件")
    return pd.concat(frames, ignore_index=True)


def hourly_to_daily(
    df: pd.DataFrame,
    *,
    timestamp_col: str = "google_trends_timestamp",
    value_col: str = "trend_index",
    keyword_col: str = "keyword_or_domain",
    geo_col: str = "country",
    agg: str = "mean",
) -> pd.DataFrame:
    """将小时级 trend_index 聚合为日级.

    Parameters
    ----------
    df : DataFrame
        小时级 Google Trends 数据 (fetch_interest_over_time 的输出格式).
    timestamp_col : str
        含完整时间戳的列名. 如果不存在, 回退到 ``date`` 列.
    value_col : str
        热度数值列.
    keyword_col : str
        关键词列.
    geo_col : str
        地域列.
    agg : str
        聚合方式, 默认 ``mean``.  可选 ``median`` / ``max`` / ``sum``.

    Returns
    -------
    DataFrame
        每行 = (keyword, geo, date, google_trends_index).
    """
    df = df.copy()

    # 解析时间戳
    if timestamp_col in df.columns:
        df["_ts"] = pd.to_datetime(df[timestamp_col], errors="coerce")
    else:
        df["_ts"] = pd.to_datetime(df["date"], errors="coerce")

    df = df.dropna(subset=["_ts"])
    df["_date"] = df["_ts"].dt.date

    # 确认 value 列是数值
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")

    group_cols = []
    if keyword_col in df.columns:
        group_cols.append(keyword_col)
    if geo_col in df.columns:
        group_cols.append(geo_col)
    group_cols.append("_date")

    daily = (
        df.groupby(group_cols, dropna=False)[value_col]
        .agg(agg)
        .reset_index()
        .rename(columns={value_col: "google_trends_index", "_date": "date"})
    )
    daily["date"] = pd.to_datetime(daily["date"])

    sort_cols = [(c if c != "_date" else "date") for c in group_cols]
    return daily.sort_values(sort_cols).reset_index(drop=True)


def build_trends_features(
    daily: pd.DataFrame,
    *,
    keyword_col: str = "keyword_or_domain",
    geo_col: str = "country",
) -> pd.DataFrame:
    """在日级数据上计算所有衍生特征.

    Parameters
    ----------
    daily : DataFrame
        ``hourly_to_daily`` 的输出, 必须包含 ``google_trends_index`` 和 ``date``.

    Returns
    -------
    DataFrame
        原始列 + 所有衍生列.
    """
    df = daily.copy()
    df = df.sort_values(
        [c for c in [keyword_col, geo_col, "date"] if c in df.columns]
    ).reset_index(drop=True)

    # 确定分组键
    group_keys = [c for c in [keyword_col, geo_col] if c in df.columns]
    val = "google_trends_index"

    if group_keys:
        g = df.groupby(group_keys, group_keys=False)[val]
    else:
        g = df[val]

    # ---- 变化量特征 ----
    if group_keys:
        df["google_trends_index_dod"] = g.diff(1)
        df["google_trends_index_wow"] = g.diff(7)
    else:
        df["google_trends_index_dod"] = df[val].diff(1)
        df["google_trends_index_wow"] = df[val].diff(7)

    # ---- 平滑特征 ----
    def _rolling_mean(s: pd.Series, window: int) -> pd.Series:
        return s.rolling(window, min_periods=1).mean()

    if group_keys:
        grouped = df.groupby(group_keys, group_keys=False)
        df["google_trends_3d_mean"] = grouped[val].transform(
            lambda s: _rolling_mean(s, 3)
        )
        df["google_trends_7d_mean"] = grouped[val].transform(
            lambda s: _rolling_mean(s, 7)
        )
    else:
        df["google_trends_3d_mean"] = _rolling_mean(df[val], 3)
        df["google_trends_7d_mean"] = _rolling_mean(df[val], 7)

    # ---- 波动特征 ----
    def _rolling_std(s: pd.Series, window: int) -> pd.Series:
        return s.rolling(window, min_periods=2).std()

    def _rolling_max(s: pd.Series, window: int) -> pd.Series:
        return s.rolling(window, min_periods=1).max()

    if group_keys:
        df["google_trends_7d_std"] = grouped[val].transform(
            lambda s: _rolling_std(s, 7)
        )
        df["google_trends_7d_max"] = grouped[val].transform(
            lambda s: _rolling_max(s, 7)
        )
    else:
        df["google_trends_7d_std"] = _rolling_std(df[val], 7)
        df["google_trends_7d_max"] = _rolling_max(df[val], 7)

    # ---- 相对位置特征 ----
    df["google_trends_vs_7d_mean_ratio"] = (
        df[val] / df["google_trends_7d_mean"]
    ).where(df["google_trends_7d_mean"] > 0)

    return df


def process_trends_csv(
    input_files: Iterable[str | Path],
    output_path: str | Path | None = None,
    *,
    agg: str = "mean",
    keyword: str | None = None,
    geo: str | None = None,
) -> pd.DataFrame:
    """一站式: 加载小时级 CSV → 日级聚合 → 衍生特征 → (可选)保存.

    Parameters
    ----------
    input_files : paths
        一个或多个 Google Trends 小时级 CSV.
    output_path : path, optional
        如指定, 将结果保存为 CSV.
    agg : str
        日级聚合方式.
    keyword : str, optional
        只保留指定关键词.
    geo : str, optional
        只保留指定地域.

    Returns
    -------
    DataFrame
        含所有衍生特征的日级数据.
    """
    raw = load_hourly_trends(input_files)

    # 过滤
    if keyword and "keyword_or_domain" in raw.columns:
        raw = raw[raw["keyword_or_domain"].str.lower() == keyword.lower()]
    if geo and "country" in raw.columns:
        raw = raw[raw["country"].str.upper() == geo.upper()]

    daily = hourly_to_daily(raw, agg=agg)
    featured = build_trends_features(daily)

    if output_path:
        p = Path(output_path).resolve()
        p.parent.mkdir(parents=True, exist_ok=True)
        featured.to_csv(p, index=False, encoding="utf-8-sig")

    return featured


def merge_trends_to_keepa(
    keepa_df: pd.DataFrame,
    trends_featured: pd.DataFrame,
    *,
    keyword: str | None = None,
    geo: str | None = None,
) -> pd.DataFrame:
    """将日级 Trends 特征合并到 Keepa 历史/特征矩阵中.

    Parameters
    ----------
    keepa_df : DataFrame
        Keepa 历史或特征矩阵, 必须含 ``date`` 列.
    trends_featured : DataFrame
        ``build_trends_features`` 的输出.
    keyword : str, optional
        指定合并哪个关键词的数据. 不指定则取第一个.
    geo : str, optional
        指定合并哪个地域的数据. 不指定则不按地域过滤.

    Returns
    -------
    DataFrame
        原始 keepa_df + trends 衍生列.
    """
    trends = trends_featured.copy()

    # 选取关键词
    if "keyword_or_domain" in trends.columns:
        if keyword:
            trends = trends[
                trends["keyword_or_domain"].str.lower() == keyword.lower()
            ]
        else:
            first_kw = trends["keyword_or_domain"].iloc[0]
            trends = trends[trends["keyword_or_domain"] == first_kw]

    # 按地域过滤
    if geo and "country" in trends.columns:
        trends = trends[trends["country"].str.upper() == geo.upper()]

    trends["date"] = pd.to_datetime(trends["date"])
    keepa = keepa_df.copy()
    keepa["date"] = pd.to_datetime(keepa["date"])

    # 只保留 trends 特征列
    trends_cols = [
        "date",
        "google_trends_index",
        "google_trends_index_dod",
        "google_trends_index_wow",
        "google_trends_3d_mean",
        "google_trends_7d_mean",
        "google_trends_7d_std",
        "google_trends_7d_max",
        "google_trends_vs_7d_mean_ratio",
    ]
    trends = trends[[c for c in trends_cols if c in trends.columns]]
    trends = trends.drop_duplicates(subset=["date"])

    return keepa.merge(trends, on="date", how="left", suffixes=("", "_gt"))
