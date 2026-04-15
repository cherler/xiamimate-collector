from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
import json

import pandas as pd


MARKETPLACE_COUNTRY_MAP = {
    "amazon us": "US",
    "amazon uk": "GB",
    "amazon de": "DE",
    "amazon fr": "FR",
    "amazon jp": "JP",
    "amazon ca": "CA",
    "amazon it": "IT",
    "amazon es": "ES",
    "temu us": "US",
    "temu uk": "GB",
    "temu de": "DE",
}

PRODUCT_NUMERIC_COLUMNS = [
    "price",
    "list_price",
    "bsr",
    "rating",
    "review_count",
    "estimated_sales",
    "seller_count",
]

TREND_NUMERIC_COLUMNS = [
    "trend_index",
    "search_volume",
    "cpc",
    "estimated_traffic",
    "backlinks",
    "referring_domains",
    "ranking_position",
]

MACRO_NUMERIC_COLUMNS = [
    "trade_value",
    "quantity",
]

XGBOOST_FEATURE_COLUMNS = [
    "price",
    "list_price",
    "bsr",
    "rating",
    "review_count",
    "seller_count",
    "trend_index",
    "search_volume",
    "estimated_traffic",
    "macro_trade_value",
    "macro_quantity",
    "price_discount_ratio",
    "day_of_week",
    "day_of_month",
    "week_of_year",
    "month",
    "estimated_sales_lag_1",
    "estimated_sales_lag_7",
    "price_lag_1",
    "price_lag_7",
    "bsr_lag_1",
    "bsr_lag_7",
    "review_count_lag_1",
    "review_count_lag_7",
    "trend_index_lag_1",
    "trend_index_lag_7",
    "estimated_sales_roll_mean_7",
    "estimated_sales_roll_mean_14",
    "price_roll_mean_7",
    "bsr_roll_mean_7",
    "review_count_roll_mean_7",
]


@dataclass
class DatasetBuildResult:
    base_dataset: pd.DataFrame
    pytorch_forecasting_dataset: pd.DataFrame
    xgboost_dataset: pd.DataFrame
    manifest: dict


class SalesForecastDatasetBuilder:
    """Build modeling datasets from collected cross-border e-commerce CSV files.

    The target is an observable sales proxy, usually `estimated_sales`, not the
    platform's real backend sales.
    """

    def __init__(self, target_column: str = "estimated_sales", min_history_rows: int = 3) -> None:
        self.target_column = target_column
        self.min_history_rows = min_history_rows

    def build(
        self,
        *,
        product_files: Iterable[str | Path],
        trend_files: Iterable[str | Path] | None = None,
        macro_files: Iterable[str | Path] | None = None,
        metadata_file: str | Path | None = None,
    ) -> DatasetBuildResult:
        product_df = self._load_product_data(product_files)
        metadata_df = self._load_metadata(metadata_file)
        trend_df = self._load_trend_data(trend_files or [])
        macro_monthly_df, macro_yearly_df = self._load_macro_data(macro_files or [])

        product_df = self._merge_metadata(product_df, metadata_df)
        product_df = self._add_join_keys(product_df)
        product_df = self._merge_trend_data(product_df, trend_df)
        product_df = self._merge_macro_data(product_df, macro_monthly_df, macro_yearly_df)
        base_dataset = self._add_time_features(product_df)
        base_dataset = self._add_group_features(base_dataset)
        base_dataset = self._finalize_base_dataset(base_dataset)

        pytorch_dataset = self._build_pytorch_forecasting_dataset(base_dataset)
        xgboost_dataset = self._build_xgboost_dataset(base_dataset)
        manifest = self._build_manifest(base_dataset, pytorch_dataset, xgboost_dataset)

        return DatasetBuildResult(
            base_dataset=base_dataset,
            pytorch_forecasting_dataset=pytorch_dataset,
            xgboost_dataset=xgboost_dataset,
            manifest=manifest,
        )

    def save(self, result: DatasetBuildResult, output_dir: str | Path) -> dict[str, Path]:
        output_path = Path(output_dir).resolve()
        output_path.mkdir(parents=True, exist_ok=True)

        base_file = output_path / "base_training_dataset.csv"
        pytorch_file = output_path / "pytorch_forecasting_dataset.csv"
        xgboost_file = output_path / "xgboost_dataset.csv"
        manifest_file = output_path / "dataset_manifest.json"

        result.base_dataset.to_csv(base_file, index=False, encoding="utf-8-sig")
        result.pytorch_forecasting_dataset.to_csv(pytorch_file, index=False, encoding="utf-8-sig")
        result.xgboost_dataset.to_csv(xgboost_file, index=False, encoding="utf-8-sig")
        manifest_file.write_text(json.dumps(result.manifest, ensure_ascii=False, indent=2), encoding="utf-8")

        return {
            "base_dataset": base_file,
            "pytorch_forecasting_dataset": pytorch_file,
            "xgboost_dataset": xgboost_file,
            "manifest": manifest_file,
        }

    def _load_product_data(self, files: Iterable[str | Path]) -> pd.DataFrame:
        frames = [self._read_csv(file_path) for file_path in files]
        if not frames:
            raise ValueError("At least one product CSV file is required.")

        frame = pd.concat(frames, ignore_index=True)
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame["data_capture_time"] = pd.to_datetime(frame.get("data_capture_time"), errors="coerce")

        for column in PRODUCT_NUMERIC_COLUMNS:
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")

        frame = frame.dropna(subset=["asin", "date"])
        frame["marketplace"] = frame["marketplace"].fillna("UNKNOWN")
        frame["keyword"] = frame.get("keyword", pd.Series(index=frame.index, dtype="object")).fillna("")
        return frame

    def _load_metadata(self, metadata_file: str | Path | None) -> pd.DataFrame | None:
        if not metadata_file:
            return None
        metadata = self._read_csv(metadata_file)
        if metadata.empty:
            return None

        if "asin" not in metadata.columns:
            raise ValueError("Metadata file must contain an 'asin' column.")

        metadata["asin"] = metadata["asin"].astype(str)
        if "marketplace" in metadata.columns:
            metadata["marketplace"] = metadata["marketplace"].fillna("")
        return metadata

    def _load_trend_data(self, files: Iterable[str | Path]) -> pd.DataFrame | None:
        frames = [self._read_csv(file_path) for file_path in files]
        if not frames:
            return None

        frame = pd.concat(frames, ignore_index=True)
        if "date" not in frame.columns:
            return None

        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        for column in TREND_NUMERIC_COLUMNS:
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")

        frame["keyword_join"] = frame.get("keyword_or_domain", pd.Series(index=frame.index, dtype="object")).fillna("").astype(str).str.strip().str.lower()
        frame["country_join"] = frame.get("country", pd.Series(index=frame.index, dtype="object")).fillna("GLOBAL").astype(str).str.upper()
        frame = frame[frame.get("data_type", "keyword").fillna("keyword").eq("keyword")]
        frame = frame.dropna(subset=["date"])
        return frame

    def _load_macro_data(self, files: Iterable[str | Path]) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
        frames = [self._read_csv(file_path) for file_path in files]
        if not frames:
            return None, None

        frame = pd.concat(frames, ignore_index=True)
        for column in MACRO_NUMERIC_COLUMNS:
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")

        frame["country_join"] = frame.get("country", pd.Series(index=frame.index, dtype="object")).fillna("").astype(str).str.upper()
        frame["hs_code_join"] = frame.get("hs_code", pd.Series(index=frame.index, dtype="object")).fillna("").astype(str)
        frame["period"] = frame.get("period", pd.Series(index=frame.index, dtype="object")).astype(str)
        frame["period_month"] = frame["period"].where(frame["period"].str.len() >= 7)
        frame["period_year"] = frame["period"].str.slice(0, 4)

        monthly = frame[frame["period_month"].notna()].copy()
        yearly = frame[frame["period_year"].notna()].copy()

        monthly = monthly.groupby(["country_join", "hs_code_join", "period_month"], as_index=False).agg(
            macro_trade_value=("trade_value", "mean"),
            macro_quantity=("quantity", "mean"),
        )
        yearly = yearly.groupby(["country_join", "hs_code_join", "period_year"], as_index=False).agg(
            macro_trade_value_year=("trade_value", "mean"),
            macro_quantity_year=("quantity", "mean"),
        )
        return monthly, yearly

    def _merge_metadata(self, product_df: pd.DataFrame, metadata_df: pd.DataFrame | None) -> pd.DataFrame:
        frame = product_df.copy()
        if metadata_df is None:
            return frame

        join_columns = ["asin"]
        if "marketplace" in metadata_df.columns:
            join_columns.append("marketplace")

        frame = frame.merge(metadata_df, how="left", on=join_columns, suffixes=("", "_meta"))
        for column in ["keyword", "country", "hs_code", "product_group_id"]:
            meta_column = f"{column}_meta"
            if meta_column in frame.columns:
                if column in frame.columns:
                    current = frame[column]
                    if pd.api.types.is_object_dtype(current):
                        current = current.replace("", pd.NA)
                    frame[column] = current.fillna(frame[meta_column])
                else:
                    frame[column] = frame[meta_column]
                frame = frame.drop(columns=[meta_column])
        return frame

    def _add_join_keys(self, product_df: pd.DataFrame) -> pd.DataFrame:
        frame = product_df.copy()
        frame["keyword"] = frame.get("keyword", pd.Series(index=frame.index, dtype="object")).fillna("")
        frame["keyword_join"] = frame["keyword"].astype(str).str.strip().str.lower()
        frame["country"] = frame.get("country", pd.Series(index=frame.index, dtype="object"))
        inferred_country = frame["marketplace"].astype(str).str.strip().str.lower().map(MARKETPLACE_COUNTRY_MAP)
        frame["country"] = frame["country"].fillna(inferred_country).fillna("GLOBAL")
        frame["country_join"] = frame["country"].astype(str).str.upper()
        frame["hs_code"] = frame.get("hs_code", pd.Series(index=frame.index, dtype="object")).fillna("").astype(str)
        frame["month_key"] = frame["date"].dt.to_period("M").astype(str)
        frame["year_key"] = frame["date"].dt.year.astype(str)
        frame["group_id"] = frame.get("product_group_id", pd.Series(index=frame.index, dtype="object"))
        frame["group_id"] = frame["group_id"].fillna(frame["marketplace"].astype(str) + "::" + frame["asin"].astype(str))
        return frame

    def _merge_trend_data(self, product_df: pd.DataFrame, trend_df: pd.DataFrame | None) -> pd.DataFrame:
        if trend_df is None:
            return product_df

        product = product_df.copy()
        trend = trend_df.copy()
        trend_global = trend[trend["country_join"].eq("GLOBAL")].copy()
        trend_specific = trend[~trend["country_join"].eq("GLOBAL")].copy()

        merged = product.merge(
            trend_specific[[
                "keyword_join",
                "country_join",
                "date",
                "trend_index",
                "search_volume",
                "cpc",
                "estimated_traffic",
                "backlinks",
                "referring_domains",
                "ranking_position",
            ]],
            how="left",
            on=["keyword_join", "country_join", "date"],
        )

        if not trend_global.empty:
            merged = merged.merge(
                trend_global[[
                    "keyword_join",
                    "date",
                    "trend_index",
                    "search_volume",
                    "cpc",
                    "estimated_traffic",
                    "backlinks",
                    "referring_domains",
                    "ranking_position",
                ]].rename(
                    columns={
                        "trend_index": "trend_index_global",
                        "search_volume": "search_volume_global",
                        "cpc": "cpc_global",
                        "estimated_traffic": "estimated_traffic_global",
                        "backlinks": "backlinks_global",
                        "referring_domains": "referring_domains_global",
                        "ranking_position": "ranking_position_global",
                    }
                ),
                how="left",
                on=["keyword_join", "date"],
            )

            for column in [
                "trend_index",
                "search_volume",
                "cpc",
                "estimated_traffic",
                "backlinks",
                "referring_domains",
                "ranking_position",
            ]:
                merged[column] = merged[column].fillna(merged.get(f"{column}_global"))
                global_column = f"{column}_global"
                if global_column in merged.columns:
                    merged = merged.drop(columns=[global_column])

        return merged

    def _merge_macro_data(
        self,
        product_df: pd.DataFrame,
        macro_monthly_df: pd.DataFrame | None,
        macro_yearly_df: pd.DataFrame | None,
    ) -> pd.DataFrame:
        merged = product_df.copy()

        if macro_monthly_df is not None:
            merged = merged.merge(
                macro_monthly_df,
                how="left",
                left_on=["country_join", "hs_code", "month_key"],
                right_on=["country_join", "hs_code_join", "period_month"],
            )
            for column in ["hs_code_join", "period_month"]:
                if column in merged.columns:
                    merged = merged.drop(columns=[column])

        if macro_yearly_df is not None:
            merged = merged.merge(
                macro_yearly_df,
                how="left",
                left_on=["country_join", "hs_code", "year_key"],
                right_on=["country_join", "hs_code_join", "period_year"],
            )
            merged["macro_trade_value"] = merged.get("macro_trade_value", pd.Series(index=merged.index, dtype="float64")).fillna(merged.get("macro_trade_value_year"))
            merged["macro_quantity"] = merged.get("macro_quantity", pd.Series(index=merged.index, dtype="float64")).fillna(merged.get("macro_quantity_year"))
            for column in ["hs_code_join", "period_year", "macro_trade_value_year", "macro_quantity_year"]:
                if column in merged.columns:
                    merged = merged.drop(columns=[column])

        return merged

    def _add_time_features(self, frame: pd.DataFrame) -> pd.DataFrame:
        result = frame.copy()
        result = result.sort_values(["group_id", "date"])
        result["target_sales"] = pd.to_numeric(result[self.target_column], errors="coerce")
        result["time_idx"] = (result["date"] - result["date"].min()).dt.days.astype(int)
        result["day_of_week"] = result["date"].dt.dayofweek.astype(int)
        result["day_of_month"] = result["date"].dt.day.astype(int)
        result["week_of_year"] = result["date"].dt.isocalendar().week.astype(int)
        result["month"] = result["date"].dt.month.astype(int)
        result["price_discount_ratio"] = (result["list_price"] - result["price"]) / result["list_price"]
        result.loc[result["list_price"].isna() | (result["list_price"] == 0), "price_discount_ratio"] = 0.0
        return result

    def _add_group_features(self, frame: pd.DataFrame) -> pd.DataFrame:
        result = frame.copy()
        grouped = result.groupby("group_id", group_keys=False)

        for column in ["target_sales", "price", "bsr", "review_count", "trend_index"]:
            if column not in result.columns:
                continue
            result[f"{column}_lag_1"] = grouped[column].shift(1)
            result[f"{column}_lag_7"] = grouped[column].shift(7)

        result["estimated_sales_roll_mean_7"] = grouped["target_sales"].transform(lambda series: series.shift(1).rolling(7, min_periods=1).mean())
        result["estimated_sales_roll_mean_14"] = grouped["target_sales"].transform(lambda series: series.shift(1).rolling(14, min_periods=1).mean())
        result["price_roll_mean_7"] = grouped["price"].transform(lambda series: series.shift(1).rolling(7, min_periods=1).mean())
        result["bsr_roll_mean_7"] = grouped["bsr"].transform(lambda series: series.shift(1).rolling(7, min_periods=1).mean())
        result["review_count_roll_mean_7"] = grouped["review_count"].transform(lambda series: series.shift(1).rolling(7, min_periods=1).mean())
        result["history_row_number"] = grouped.cumcount() + 1
        result["is_target_available"] = result["target_sales"].notna().astype(int)
        return result

    def _finalize_base_dataset(self, frame: pd.DataFrame) -> pd.DataFrame:
        result = frame.copy()
        result = result.sort_values(["group_id", "date"]).reset_index(drop=True)
        result["date"] = result["date"].dt.strftime("%Y-%m-%d")
        return result

    def _build_pytorch_forecasting_dataset(self, base_dataset: pd.DataFrame) -> pd.DataFrame:
        frame = base_dataset.copy()
        frame = frame[frame["history_row_number"] >= self.min_history_rows].copy()
        frame = frame[frame["target_sales"].notna()].copy()

        frame["series_id"] = frame["group_id"]
        frame["static_marketplace"] = frame["marketplace"].fillna("UNKNOWN")
        frame["static_category"] = frame.get("category", pd.Series(index=frame.index, dtype="object")).fillna("UNKNOWN")
        frame["static_brand"] = frame.get("brand", pd.Series(index=frame.index, dtype="object")).fillna("UNKNOWN")
        frame["static_country"] = frame.get("country", pd.Series(index=frame.index, dtype="object")).fillna("GLOBAL")
        frame["known_month"] = frame["month"]
        frame["known_day_of_week"] = frame["day_of_week"]
        frame["known_trend_index"] = frame.get("trend_index")
        frame["known_macro_trade_value"] = frame.get("macro_trade_value")
        frame["unknown_price"] = frame.get("price")
        frame["unknown_bsr"] = frame.get("bsr")
        frame["unknown_review_count"] = frame.get("review_count")
        frame["target"] = frame["target_sales"]

        preferred_columns = [
            "series_id",
            "time_idx",
            "date",
            "target",
            "static_marketplace",
            "static_category",
            "static_brand",
            "static_country",
            "known_month",
            "known_day_of_week",
            "known_trend_index",
            "known_macro_trade_value",
            "unknown_price",
            "unknown_bsr",
            "unknown_review_count",
            "estimated_sales_lag_1",
            "estimated_sales_lag_7",
            "estimated_sales_roll_mean_7",
            "estimated_sales_roll_mean_14",
            "group_id",
            "asin",
            "keyword",
            "country",
            "hs_code",
        ]
        return self._select_existing_columns(frame, preferred_columns)

    def _build_xgboost_dataset(self, base_dataset: pd.DataFrame) -> pd.DataFrame:
        frame = base_dataset.copy()
        frame = frame[frame["history_row_number"] >= self.min_history_rows].copy()
        frame = frame[frame["target_sales"].notna()].copy()
        frame["target"] = frame["target_sales"]

        base_columns = [
            "group_id",
            "asin",
            "marketplace",
            "brand",
            "category",
            "keyword",
            "country",
            "hs_code",
            "date",
            "time_idx",
            "target",
        ]
        feature_columns = [column for column in XGBOOST_FEATURE_COLUMNS if column in frame.columns]
        return self._select_existing_columns(frame, base_columns + feature_columns)

    def _build_manifest(
        self,
        base_dataset: pd.DataFrame,
        pytorch_dataset: pd.DataFrame,
        xgboost_dataset: pd.DataFrame,
    ) -> dict:
        return {
            "target_column": self.target_column,
            "target_definition": "Observable sales proxy, usually estimated_sales, not platform backend sales.",
            "base_dataset_rows": int(len(base_dataset)),
            "pytorch_forecasting_rows": int(len(pytorch_dataset)),
            "xgboost_rows": int(len(xgboost_dataset)),
            "unique_groups": int(base_dataset["group_id"].nunique()) if not base_dataset.empty else 0,
            "date_min": str(base_dataset["date"].min()) if not base_dataset.empty else None,
            "date_max": str(base_dataset["date"].max()) if not base_dataset.empty else None,
            "features_for_xgboost": [column for column in XGBOOST_FEATURE_COLUMNS if column in base_dataset.columns],
            "notes": [
                "Use the xgboost dataset for tabular regression models.",
                "Use the pytorch forecasting dataset as the long-format input for TimeSeriesDataSet.",
                "Rows without enough history or without target_sales are removed from training outputs.",
            ],
        }

    @staticmethod
    def _read_csv(file_path: str | Path) -> pd.DataFrame:
        path = Path(file_path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {path}")
        frame = pd.read_csv(path)
        frame["source_file"] = str(path)
        return frame

    @staticmethod
    def _select_existing_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        existing = [column for column in columns if column in frame.columns]
        remaining = [column for column in frame.columns if column not in existing]
        return frame[existing + remaining].copy()
