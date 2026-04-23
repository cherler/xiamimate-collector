from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
import sys

from dotenv import load_dotenv

from .dataset_builder import SalesForecastDatasetBuilder
from .feature_matrix import FeatureMatrixBuilder
from .week1_feature_foundation import (
    Week1FeatureFoundationBuilder,
    build_domain_output_dir,
    discover_available_domains,
)


load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if not hasattr(args, "handler"):
        parser.print_help()
        return

    args.handler(args)


def _parse_domains(raw_domains: str | None) -> list[int]:
    if not raw_domains:
        return []
    domains: list[int] = []
    for item in raw_domains.split(","):
        stripped = item.strip()
        if not stripped:
            continue
        domains.append(int(stripped))
    return domains


def _build_week1_domain_job(
    *,
    source_db_path: Path | None,
    output_dir: Path | None,
    domain: int,
    active_only: bool,
    duckdb_threads: int | None,
    feature_profile: str,
) -> tuple[int, dict[str, str]]:
    builder = Week1FeatureFoundationBuilder(
        source_db_path=source_db_path,
        output_dir=build_domain_output_dir(output_dir, domain),
        domain=domain,
        active_only=active_only,
        duckdb_threads=duckdb_threads,
        feature_profile=feature_profile,
    )
    outputs = builder.build()
    return domain, {name: str(path) for name, path in outputs.items()}


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
    foundation.add_argument(
        "--domains",
        default=None,
        help="逗号分隔多个站点；指定后会按站点分别构建并落地到子目录",
    )
    foundation.add_argument(
        "--split-by-domain",
        action="store_true",
        help="自动发现源库中的站点，并按站点分别构建落地",
    )
    foundation.add_argument("--active-only", action="store_true", help="仅构建当前活跃 ASIN")
    foundation.add_argument(
        "--duckdb-threads",
        type=int,
        default=None,
        help="每个构建进程内部 DuckDB 线程数；默认读取 WEEK1_FOUNDATION_DUCKDB_THREADS",
    )
    foundation.add_argument(
        "--max-workers",
        type=int,
        default=1,
        help="按域并行构建的最大进程数；仅在 --domains/--split-by-domain 下生效",
    )
    foundation.add_argument(
        "--feature-profile",
        choices=["full", "base"],
        default="full",
        help="full=基础+趋势+交叉+训练集，base=仅基础时序特征，跳过重的趋势/交叉阶段",
    )
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
    if args.domain is not None and args.domains:
        raise SystemExit("--domain 与 --domains 不能同时使用")

    explicit_domains = _parse_domains(args.domains)
    source_db_path = Path(args.source_db) if args.source_db else None
    output_dir = Path(args.output_dir) if args.output_dir else None

    target_domains: list[int] = []
    if args.domain is not None:
        target_domains = [args.domain]
    elif explicit_domains:
        target_domains = explicit_domains
    elif args.split_by_domain:
        target_domains = discover_available_domains(
            source_db_path,
            active_only=args.active_only,
        )

    if target_domains:
        max_workers = max(1, args.max_workers)
        print(
            "Building week1 feature foundation per domain: "
            f"{target_domains} (max_workers={min(max_workers, len(target_domains))}, "
            f"duckdb_threads={args.duckdb_threads or 'env/default'}, feature_profile={args.feature_profile})"
        )

        if max_workers == 1 or len(target_domains) == 1:
            for domain in target_domains:
                built_domain, outputs = _build_week1_domain_job(
                    source_db_path=source_db_path,
                    output_dir=output_dir,
                    domain=domain,
                    active_only=args.active_only,
                    duckdb_threads=args.duckdb_threads,
                    feature_profile=args.feature_profile,
                )
                print(f"Domain {built_domain} outputs:")
                for name, path in outputs.items():
                    print(f"- {name}: {path}")
            return

        with ProcessPoolExecutor(max_workers=min(max_workers, len(target_domains))) as executor:
            future_map = {
                executor.submit(
                    _build_week1_domain_job,
                    source_db_path=source_db_path,
                    output_dir=output_dir,
                    domain=domain,
                    active_only=args.active_only,
                    duckdb_threads=args.duckdb_threads,
                    feature_profile=args.feature_profile,
                ): domain
                for domain in target_domains
            }
            for future in as_completed(future_map):
                built_domain, outputs = future.result()
                print(f"Domain {built_domain} outputs:")
                for name, path in outputs.items():
                    print(f"- {name}: {path}")
        return

    builder = Week1FeatureFoundationBuilder(
        source_db_path=source_db_path,
        output_dir=output_dir,
        domain=args.domain,
        active_only=args.active_only,
        duckdb_threads=args.duckdb_threads,
        feature_profile=args.feature_profile,
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
