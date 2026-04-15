from __future__ import annotations

import argparse
from pathlib import Path
import sys

from .dataset_builder import SalesForecastDatasetBuilder
from .feature_matrix import FeatureMatrixBuilder
from .week1_feature_foundation import Week1FeatureFoundationBuilder


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if not hasattr(args, "handler"):
        parser.print_help()
        return

    args.handler(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m data_collector.sales_forecast",
        description="跨境电商销量预测数据集构建工具",
    )
    subparsers = parser.add_subparsers(dest="command")

    build = subparsers.add_parser("build-dataset", help="构建销量预测训练数据集")
    build.add_argument("--product-file", action="append", required=True, help="标准化商品监控 CSV，可重复传入")
    build.add_argument("--trend-file", action="append", default=[], help="标准化趋势 CSV，可重复传入")
    build.add_argument("--macro-file", action="append", default=[], help="标准化宏观贸易 CSV，可重复传入")
    build.add_argument("--metadata-file", help="商品元数据映射 CSV，例如 asin -> keyword/country/hs_code")
    build.add_argument("--target-column", default="estimated_sales", help="用于建模的目标列，默认 estimated_sales")
    build.add_argument("--min-history-rows", type=int, default=3, help="最少历史样本行数，默认 3")
    build.add_argument("--output-dir", required=True, help="输出目录")
    build.set_defaults(handler=handle_build_dataset)

    matrix = subparsers.add_parser("build-feature-matrix", help="构建预测特征矩阵 (Keepa历史 + Google Trends + BSR→日销量)")
    matrix.add_argument("--keepa-file", action="append", required=True, help="Keepa 历史 CSV (keepa-history 输出), 可重复传入")
    matrix.add_argument("--trend-file", action="append", default=[], help="Google Trends CSV, 可重复传入")
    matrix.add_argument("--metadata-file", help="商品元数据映射 CSV (asin → keyword/hs_code)")
    matrix.add_argument("--domain", type=int, default=1, help="Amazon 站点 (1=US, 2=UK, 3=DE, 5=JP, 6=CA)")
    matrix.add_argument("--category-id", type=int, default=None, help="根品类 ID, 用于 BSR→销量系数 (不指定则用通用系数)")
    matrix.add_argument("--trend-keyword", default=None, help="Google Trends 中要匹配的关键词")
    matrix.add_argument("--no-ffill", action="store_true", help="不做前向填充 (默认会 ffill)")
    matrix.add_argument("--output-dir", required=True, help="输出目录")
    matrix.set_defaults(handler=handle_build_feature_matrix)

    foundation = subparsers.add_parser(
        "build-week1-foundation",
        help="从 DuckDB 构建第一周 P0 数据与特征基建产物",
    )
    foundation.add_argument(
        "--source-db",
        default=None,
        help="DuckDB 数据仓库路径，默认 data_platform/storage/warehouse/local_analytics.duckdb",
    )
    foundation.add_argument(
        "--output-dir",
        default=None,
        help="输出目录，默认 data_platform/storage/features/training_sets/week1_foundation",
    )
    foundation.add_argument("--domain", type=int, default=None, help="仅构建指定站点")
    foundation.add_argument("--active-only", action="store_true", help="仅构建当前活跃 ASIN")
    foundation.set_defaults(handler=handle_build_week1_foundation)

    return parser


def handle_build_dataset(args: argparse.Namespace) -> None:
    builder = SalesForecastDatasetBuilder(
        target_column=args.target_column,
        min_history_rows=args.min_history_rows,
    )
    result = builder.build(
        product_files=[Path(path) for path in args.product_file],
        trend_files=[Path(path) for path in args.trend_file],
        macro_files=[Path(path) for path in args.macro_file],
        metadata_file=Path(args.metadata_file) if args.metadata_file else None,
    )
    saved_files = builder.save(result, args.output_dir)

    print("Built forecasting datasets:")
    for name, path in saved_files.items():
        print(f"- {name}: {path}")
    print(f"- base rows: {len(result.base_dataset)}")
    print(f"- pytorch rows: {len(result.pytorch_forecasting_dataset)}")
    print(f"- xgboost rows: {len(result.xgboost_dataset)}")


def handle_build_feature_matrix(args: argparse.Namespace) -> None:
    builder = FeatureMatrixBuilder(
        domain=args.domain,
        category_id=args.category_id,
        fill_method="none" if args.no_ffill else "ffill",
        trend_keyword=args.trend_keyword,
    )
    df = builder.build(
        keepa_history_files=[Path(p) for p in args.keepa_file],
        trend_files=[Path(p) for p in args.trend_file] if args.trend_file else None,
        metadata_file=Path(args.metadata_file) if args.metadata_file else None,
    )
    saved = builder.save(df, args.output_dir)

    print(f"Built feature matrix: {len(df)} rows, {len(df.columns)} columns")
    for name, path in saved.items():
        print(f"  {name}: {path}")
    if "sales_estimation_method" in df.columns:
        methods = df["sales_estimation_method"].value_counts()
        print("Sales estimation methods:")
        for method, count in methods.items():
            print(f"  {method}: {count} rows")


def handle_build_week1_foundation(args: argparse.Namespace) -> None:
    builder = Week1FeatureFoundationBuilder(
        source_db_path=Path(args.source_db) if args.source_db else None,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        domain=args.domain,
        active_only=args.active_only,
    )
    outputs = builder.build()

    print("Built week1 feature foundation:")
    for name, path in outputs.items():
        print(f"- {name}: {path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
