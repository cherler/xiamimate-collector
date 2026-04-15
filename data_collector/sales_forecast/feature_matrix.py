"""预测特征矩阵构建器.

将 Keepa 历史数据 + Google Trends 热度数据 + BSR→日销量估算 合并为
一个按日对齐的特征矩阵, 供后续 XGBoost / PyTorch Forecasting 建模使用.

输入:
  1. Keepa 历史 CSV (keepa-history 命令输出)
  2. Google Trends CSV (google-trends 命令输出)
  3. 可选: 商品元数据 CSV (asin → keyword/hs_code 映射)

输出:
  prediction_feature_matrix.csv — 逐日特征矩阵
  feature_matrix_manifest.json — 数据集描述清单
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable
import json
import math

import pandas as pd

from .bsr_sales_converter import (
    CATEGORY_COEFFICIENTS,
    bsr_to_daily_sales,
    estimate_daily_sales,
    monthly_sold_to_daily_sales,
)
from .trends_features import hourly_to_daily, build_trends_features, merge_trends_to_keepa
from ..cross_border_data.collectors.product import KEEPA_DOMAIN_TO_GEO


# Keepa domain → Amazon marketplace 标签
_MARKETPLACE_COUNTRY = {
    "amazon us": "US",
    "amazon uk": "GB",
    "amazon de": "DE",
    "amazon fr": "FR",
    "amazon jp": "JP",
    "amazon ca": "CA",
    "amazon it": "IT",
    "amazon es": "ES",
}

# Keepa root category ID → 品类名
_CATEGORY_ID_NAMES = {c.category_id: c.category_name for c in CATEGORY_COEFFICIENTS.values()}


class FeatureMatrixBuilder:
    """构建预测特征矩阵."""

    def __init__(
        self,
        *,
        domain: int = 1,
        category_id: int | None = None,
        fill_method: str = "ffill",
        trend_keyword: str | None = None,
    ) -> None:
        """
        Parameters
        ----------
        domain : int
            Keepa domain ID, 用于 BSR→sales 站点倍率.
        category_id : int, optional
            根品类 ID, 用于 BSR→sales 品类系数.
        fill_method : str
            稀疏字段的填充方式: "ffill"(向前填充) 或 "none"(不填充).
        trend_keyword : str, optional
            Google Trends 中的匹配关键词. 如果不指定, 会取第一个关键词.
        """
        self.domain = domain
        self.category_id = category_id
        self.fill_method = fill_method
        self.trend_keyword = trend_keyword

    def build(
        self,
        *,
        keepa_history_files: Iterable[str | Path],
        trend_files: Iterable[str | Path] | None = None,
        metadata_file: str | Path | None = None,
    ) -> pd.DataFrame:
        """构建特征矩阵并返回 DataFrame."""
        keepa_df = self._load_keepa_history(keepa_history_files)
        trend_df = self._load_trends(trend_files or [])
        metadata_df = self._load_metadata(metadata_file)

        # 1. 基础清洗与类型转换
        keepa_df = self._clean_keepa(keepa_df)

        # 2. 前向填充稀疏数据
        if self.fill_method == "ffill":
            keepa_df = self._forward_fill(keepa_df)

        # 3. BSR → 日销量估算
        keepa_df = self._add_sales_estimates(keepa_df)

        # 4. 派生特征
        keepa_df = self._add_derived_features(keepa_df)

        # 5. 合并 Google Trends
        if trend_df is not None:
            keepa_df = self._merge_trends(keepa_df, trend_df)

        # 6. 合并元数据
        if metadata_df is not None:
            keepa_df = self._merge_metadata(keepa_df, metadata_df)

        # 7. 添加时间特征
        keepa_df = self._add_time_features(keepa_df)

        # 8. 添加滞后/滚动特征
        keepa_df = self._add_lag_rolling_features(keepa_df)

        return keepa_df.reset_index(drop=True)

    def save(
        self,
        df: pd.DataFrame,
        output_dir: str | Path,
        prefix: str = "prediction_feature_matrix",
    ) -> dict[str, Path]:
        """保存特征矩阵和清单."""
        out = Path(output_dir).resolve()
        out.mkdir(parents=True, exist_ok=True)

        csv_path = out / f"{prefix}.csv"
        manifest_path = out / f"{prefix}_manifest.json"

        df.to_csv(csv_path, index=False, encoding="utf-8-sig")

        manifest = self._build_manifest(df)
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return {"feature_matrix": csv_path, "manifest": manifest_path}

    # ------------------------------------------------------------------
    # 数据加载
    # ------------------------------------------------------------------

    @staticmethod
    def _load_keepa_history(files: Iterable[str | Path]) -> pd.DataFrame:
        frames = []
        for f in files:
            path = Path(f).resolve()
            if not path.exists():
                raise FileNotFoundError(f"Keepa history file not found: {path}")
            frames.append(pd.read_csv(path))
        if not frames:
            raise ValueError("至少需要一个 Keepa 历史 CSV 文件")
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _load_trends(files: Iterable[str | Path]) -> pd.DataFrame | None:
        frames = []
        for f in files:
            path = Path(f).resolve()
            if path.exists():
                frames.append(pd.read_csv(path))
        if not frames:
            return None
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _load_metadata(path: str | Path | None) -> pd.DataFrame | None:
        if not path:
            return None
        p = Path(path).resolve()
        if not p.exists():
            return None
        return pd.read_csv(p)

    # ------------------------------------------------------------------
    # Keepa 数据清洗
    # ------------------------------------------------------------------

    def _clean_keepa(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date", "asin"])
        df = df.sort_values(["asin", "date"]).reset_index(drop=True)

        numeric_cols = [
            "amazon_price", "new_price", "used_price", "buy_box_price",
            "list_price", "bsr", "rating", "review_count", "monthly_sold",
            "new_offer_count", "used_offer_count",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def _forward_fill(self, df: pd.DataFrame) -> pd.DataFrame:
        """对每个 ASIN 的稀疏列做前向填充.

        Keepa 记录的是"变化点": 只有值发生变化时才记录一条数据.
        因此同一天只有部分字段有值, 其他为空. 前向填充将上一次的值延续到当前日期.
        """
        df = df.copy()

        # 需要前向填充的列 (持续性数据, 不是事件型数据)
        ffill_cols = [
            "amazon_price", "new_price", "used_price", "buy_box_price",
            "list_price", "bsr", "rating", "review_count",
            "new_offer_count", "used_offer_count",
        ]
        existing_cols = [c for c in ffill_cols if c in df.columns]

        df[existing_cols] = df.groupby("asin")[existing_cols].ffill()
        return df

    # ------------------------------------------------------------------
    # BSR → 日销量
    # ------------------------------------------------------------------

    def _add_sales_estimates(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # BSR 幂律日销量
        df["est_daily_sales_bsr"] = df["bsr"].apply(
            lambda x: bsr_to_daily_sales(
                x, category_id=self.category_id, domain=self.domain
            )
        )

        # monthly_sold → 日销量
        if "monthly_sold" in df.columns:
            df["est_daily_sales_monthly"] = df["monthly_sold"].apply(
                monthly_sold_to_daily_sales
            )
        else:
            df["est_daily_sales_monthly"] = None

        # 综合估算: 优先 monthly_sold > BSR
        def _pick_best(row):
            daily, method = estimate_daily_sales(
                bsr=row.get("bsr"),
                monthly_sold=row.get("monthly_sold"),
                category_id=self.category_id,
                domain=self.domain,
            )
            return pd.Series({"estimated_daily_sales": daily, "sales_method": method})

        estimates = df.apply(_pick_best, axis=1)
        df["estimated_daily_sales"] = estimates["estimated_daily_sales"]
        df["sales_estimation_method"] = estimates["sales_method"]

        return df

    # ------------------------------------------------------------------
    # 派生特征
    # ------------------------------------------------------------------

    def _add_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # 价格折扣率
        if "list_price" in df.columns and "amazon_price" in df.columns:
            df["price_discount_pct"] = (
                (df["list_price"] - df["amazon_price"]) / df["list_price"] * 100
            ).where(df["list_price"] > 0)

        # 实际销售价 (优先 buy_box > amazon > new)
        df["effective_price"] = (
            df.get("buy_box_price")
            .fillna(df.get("amazon_price"))
            .fillna(df.get("new_price"))
        )

        # BSR 变化率 (当日 vs 前一日)
        df["bsr_change"] = df.groupby("asin")["bsr"].diff()
        df["bsr_change_pct"] = (
            df["bsr_change"] / df.groupby("asin")["bsr"].shift(1) * 100
        )

        # 价格变化率
        df["price_change"] = df.groupby("asin")["effective_price"].diff()
        df["price_change_pct"] = (
            df["price_change"] / df.groupby("asin")["effective_price"].shift(1) * 100
        )

        # 评论增速
        if "review_count" in df.columns:
            df["review_velocity"] = df.groupby("asin")["review_count"].diff()

        # BSR 对数 (幂律关系在对数空间是线性的, 更适合建模)
        if "bsr" in df.columns:
            df["log_bsr"] = df["bsr"].apply(
                lambda x: math.log1p(x) if pd.notna(x) and x > 0 else None
            )

        return df

    # ------------------------------------------------------------------
    # Google Trends 合并
    # ------------------------------------------------------------------

    def _merge_trends(self, keepa_df: pd.DataFrame, trend_df: pd.DataFrame) -> pd.DataFrame:
        trend = trend_df.copy()

        # 判断是小时级还是日级数据: 检查是否有 google_trends_timestamp 列或
        # 同一天内有多条记录
        is_hourly = "google_trends_timestamp" in trend.columns
        if not is_hourly and "date" in trend.columns:
            trend["_date_tmp"] = pd.to_datetime(trend["date"], errors="coerce").dt.date
            if "keyword_or_domain" in trend.columns:
                g = trend.groupby(["keyword_or_domain", "_date_tmp"]).size()
            else:
                g = trend.groupby("_date_tmp").size()
            is_hourly = (g > 1).any()
            trend = trend.drop(columns=["_date_tmp"])

        if is_hourly:
            # 小时级 → 日级聚合 + 衍生特征
            daily = hourly_to_daily(trend)
            featured = build_trends_features(daily)
            geo = KEEPA_DOMAIN_TO_GEO.get(self.domain, "")
            return merge_trends_to_keepa(
                keepa_df, featured, keyword=self.trend_keyword, geo=geo or None,
            )

        # 回退: 传统周级/日级数据 → 线性插值
        trend["date"] = pd.to_datetime(trend["date"], errors="coerce")
        trend = trend.dropna(subset=["date"])

        # 选取关键词
        if self.trend_keyword:
            trend = trend[
                trend["keyword_or_domain"].str.lower() == self.trend_keyword.lower()
            ]
        elif "keyword_or_domain" in trend.columns and not trend.empty:
            first_kw = trend["keyword_or_domain"].iloc[0]
            trend = trend[trend["keyword_or_domain"] == first_kw]

        trend_cols = ["date", "trend_index"]
        if "search_volume" in trend.columns:
            trend_cols.append("search_volume")

        trend = trend[trend_cols].drop_duplicates(subset=["date"])
        trend = trend.set_index("date").resample("D").interpolate(method="linear")
        trend = trend.reset_index()
        trend = trend.rename(columns={"trend_index": "google_trends_index"})

        keepa_df = keepa_df.merge(trend, on="date", how="left", suffixes=("", "_trend"))
        return keepa_df

    # ------------------------------------------------------------------
    # 元数据合并
    # ------------------------------------------------------------------

    def _merge_metadata(self, df: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
        if "asin" not in meta.columns:
            return df
        join_cols = ["asin"]
        if "marketplace" in meta.columns:
            join_cols.append("marketplace")
        return df.merge(meta, on=join_cols, how="left", suffixes=("", "_meta"))

    # ------------------------------------------------------------------
    # 时间特征
    # ------------------------------------------------------------------

    @staticmethod
    def _add_time_features(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["day_of_week"] = df["date"].dt.dayofweek
        df["day_of_month"] = df["date"].dt.day
        df["week_of_year"] = df["date"].dt.isocalendar().week.astype(int)
        df["month"] = df["date"].dt.month
        df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)

        # 距离时间序列起点的天数 (time_idx)
        df["time_idx"] = (df["date"] - df["date"].min()).dt.days
        return df

    # ------------------------------------------------------------------
    # 滞后 & 滚动特征
    # ------------------------------------------------------------------

    @staticmethod
    def _add_lag_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        grouped = df.groupby("asin", group_keys=False)

        for col in ["estimated_daily_sales", "bsr", "effective_price", "review_count"]:
            if col not in df.columns:
                continue
            df[f"{col}_lag_1"] = grouped[col].shift(1)
            df[f"{col}_lag_7"] = grouped[col].shift(7)
            df[f"{col}_lag_14"] = grouped[col].shift(14)
            df[f"{col}_lag_30"] = grouped[col].shift(30)

        # 滚动均值
        for col in ["estimated_daily_sales", "bsr"]:
            if col not in df.columns:
                continue
            df[f"{col}_roll_mean_7"] = grouped[col].transform(
                lambda s: s.shift(1).rolling(7, min_periods=1).mean()
            )
            df[f"{col}_roll_mean_14"] = grouped[col].transform(
                lambda s: s.shift(1).rolling(14, min_periods=1).mean()
            )
            df[f"{col}_roll_mean_30"] = grouped[col].transform(
                lambda s: s.shift(1).rolling(30, min_periods=1).mean()
            )
            # 滚动标准差 (波动性)
            df[f"{col}_roll_std_7"] = grouped[col].transform(
                lambda s: s.shift(1).rolling(7, min_periods=2).std()
            )

        # trend_index 滚动 (向后兼容: 如果是传统路径合并的, 无 google_trends_index)
        if "google_trends_index" in df.columns:
            df["google_trends_index_lag_7"] = grouped["google_trends_index"].shift(7)
            # 如果 trends_features 已经计算了衍生列, 这里不重复
            if "google_trends_7d_mean" not in df.columns:
                df["google_trends_7d_mean"] = grouped["google_trends_index"].transform(
                    lambda s: s.shift(1).rolling(7, min_periods=1).mean()
                )
        elif "trend_index" in df.columns:
            df["trend_index_lag_7"] = grouped["trend_index"].shift(7)
            df["trend_index_roll_mean_7"] = grouped["trend_index"].transform(
                lambda s: s.shift(1).rolling(7, min_periods=1).mean()
            )

        return df

    # ------------------------------------------------------------------
    # 清单
    # ------------------------------------------------------------------

    @staticmethod
    def _build_manifest(df: pd.DataFrame) -> dict:
        feature_cols = [c for c in df.columns if c not in (
            "asin", "product_title", "brand", "category", "marketplace", "date",
            "sales_estimation_method",
        )]
        return {
            "description": "预测特征矩阵: Keepa 历史数据 + Google Trends + BSR→日销量",
            "total_rows": int(len(df)),
            "unique_asins": int(df["asin"].nunique()),
            "date_range": {
                "min": str(df["date"].min().date()) if not df.empty else None,
                "max": str(df["date"].max().date()) if not df.empty else None,
            },
            "feature_columns": feature_cols,
            "feature_count": len(feature_cols),
            "sales_estimation_methods": (
                df["sales_estimation_method"].value_counts().to_dict()
                if "sales_estimation_method" in df.columns else {}
            ),
            "notes": [
                "estimated_daily_sales: 综合估算日销量 (优先 monthly_sold > BSR 幂律模型)",
                "est_daily_sales_bsr: 仅基于 BSR 幂律模型的估算",
                "trend_index: Google Trends 日级搜索热度 (小时级聚合为日均值, 含 dod/wow/3d/7d 衍生特征)",
                "前向填充 (ffill): Keepa 只在数据变化时记录, 空值已用上一个已知值填充",
                "lag/rolling 特征使用 shift(1) 避免未来信息泄漏",
            ],
        }
