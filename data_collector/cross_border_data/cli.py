from __future__ import annotations

import argparse
import fcntl
import os
from pathlib import Path
import sys

from .config import load_settings
from .utils import write_json, write_rows
from .collectors import (
    AhrefsCollector,
    EurostatCollector,
    GoogleTrendsCollector,
    KeepaCollector,
    SemrushCollector,
    SellerSpriteImporter,
    UNComtradeCollector,
    USCensusCollector,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LOG_DIR = Path(
    os.environ.get("XIAMIMATE_LOG_DIR", PROJECT_ROOT / "logs")
).expanduser().resolve()
DEFAULT_PRODUCTS_DIR = Path(
    os.environ.get(
        "XIAMIMATE_RAW_PRODUCTS_DIR",
        PROJECT_ROOT / "data_platform" / "storage" / "raw" / "json" / "products",
    )
).expanduser().resolve()
DEFAULT_AUTO_COLLECT_LOCK_PATH = DEFAULT_LOG_DIR / "auto_collect.lock"


def acquire_process_lock(lock_path: str | Path):
    """Acquire a non-blocking file lock so only one auto-collect process can run."""
    path = Path(lock_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("w", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        sys.exit(f"auto-collect 已在运行, 请勿重复启动: {path}")

    handle.write(f"pid={os.getpid()}\n")
    handle.flush()
    return handle


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if not hasattr(args, "handler"):
        parser.print_help()
        return

    settings = load_settings()
    args.handler(args, settings)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m data_collector.cross_border_data",
        description="跨境电商数据采集工具",
    )
    subparsers = parser.add_subparsers(dest="command")

    census = subparsers.add_parser("census", help="拉取 US Census HS 进口数据")
    census.add_argument("--time", required=True, help="统计周期，例如 2025-12")
    census.add_argument("--country-code", required=True, help="贸易伙伴国编码，例如 1220")
    census.add_argument(
        "--fields",
        default="I_COMMODITY,I_COMMODITY_SDESC,GEN_VAL_MO,GEN_VAL_YR,CTY_NAME",
        help="逗号分隔的字段列表",
    )
    census.add_argument("--param", action="append", default=[], help="附加参数，格式 key=value")
    census.add_argument("--output", required=True, help="输出 CSV 文件路径")
    census.set_defaults(handler=handle_census)

    eurostat = subparsers.add_parser("eurostat", help="拉取 Eurostat 数据集")
    eurostat.add_argument("--dataset", required=True, help="数据集编码，例如 ext_lt_intertrd")
    eurostat.add_argument("--param", action="append", default=[], help="查询参数，格式 key=value")
    eurostat.add_argument("--output", required=True, help="输出 CSV 文件路径")
    eurostat.set_defaults(handler=handle_eurostat)

    comtrade = subparsers.add_parser("comtrade", help="拉取 UN Comtrade 数据")
    comtrade.add_argument("--reporter-code", required=True)
    comtrade.add_argument("--partner-code", required=True)
    comtrade.add_argument("--flow-code", required=True, help="例如 M 或 X")
    comtrade.add_argument("--cmd-code", required=True, help="例如 TOTAL 或 420292")
    comtrade.add_argument("--period", required=True, help="例如 2025 或 2025-12")
    comtrade.add_argument("--param", action="append", default=[], help="附加参数，格式 key=value")
    comtrade.add_argument("--output", required=True, help="输出 CSV 文件路径")
    comtrade.set_defaults(handler=handle_comtrade)

    google_trends = subparsers.add_parser("google-trends", help="拉取 Google Trends 数据")
    google_trends.add_argument("--keyword", action="append", required=True, help="可重复传入多个关键词")
    google_trends.add_argument("--geo", default="", help="地区代码，例如 US")
    google_trends.add_argument("--timeframe", default="today 12-m")
    google_trends.add_argument("--category", type=int, default=0)
    google_trends.add_argument("--gprop", default="")
    google_trends.add_argument("--output", required=True, help="输出 CSV 文件路径")
    google_trends.set_defaults(handler=handle_google_trends)

    semrush = subparsers.add_parser("semrush-domain-history", help="拉取 Semrush 域名历史概览")
    semrush.add_argument("--domain", required=True)
    semrush.add_argument("--database", default="us")
    semrush.add_argument("--report-type", default="domain_rank_history")
    semrush.add_argument("--display-limit", type=int, default=120)
    semrush.add_argument("--export-columns", default="Rk,Or,Ot,Oc,Ad,At,Ac,Dt")
    semrush.add_argument("--daily", action="store_true")
    semrush.add_argument("--output", required=True, help="输出 CSV 文件路径")
    semrush.set_defaults(handler=handle_semrush)

    ahrefs = subparsers.add_parser("ahrefs-site-overview", help="拉取 Ahrefs 站点概览")
    ahrefs.add_argument("--target", required=True)
    ahrefs.add_argument("--country", default="us")
    ahrefs.add_argument("--mode", default="subdomains")
    ahrefs.add_argument("--endpoint-path", default=None)
    ahrefs.add_argument("--param", action="append", default=[], help="附加参数，格式 key=value")
    ahrefs.add_argument("--output", required=True, help="输出 CSV 文件路径")
    ahrefs.set_defaults(handler=handle_ahrefs)

    keepa = subparsers.add_parser("keepa-products", help="拉取 Keepa 商品数据")
    keepa.add_argument("--asin", action="append", required=True, help="可重复传入多个 ASIN")
    keepa.add_argument("--domain", type=int, default=1)
    keepa.add_argument("--no-history", action="store_true")
    keepa.add_argument("--stats-window-days", type=int, default=90)
    keepa.add_argument("--output", required=True, help="输出标准化 CSV 文件路径")
    keepa.add_argument("--raw-output", help="可选，输出原始 JSON 文件路径")
    keepa.set_defaults(handler=handle_keepa)

    keepa_history = subparsers.add_parser("keepa-history", help="拉取 Keepa 商品历史数据（价格/BSR/评论/销量时间序列）")
    keepa_history.add_argument("--asin", action="append", required=True, help="可重复传入多个 ASIN")
    keepa_history.add_argument("--domain", type=int, default=1, help="Amazon 站点编号，1=US, 2=UK, 3=DE, 5=JP, 6=CA")
    keepa_history.add_argument("--stats-window-days", type=int, default=90)
    keepa_history.add_argument("--output", required=True, help="输出历史数据 CSV 文件路径")
    keepa_history.add_argument("--raw-output", help="可选，输出原始 JSON 文件路径")
    keepa_history.set_defaults(handler=handle_keepa_history)

    keepa_tokens = subparsers.add_parser("keepa-tokens", help="查询 Keepa API 剩余 token 额度")
    keepa_tokens.set_defaults(handler=handle_keepa_tokens)

    sellersprite = subparsers.add_parser("import-sellersprite", help="导入卖家精灵导出文件")
    sellersprite.add_argument("--input", required=True, help="卖家精灵导出 CSV 路径")
    sellersprite.add_argument("--marketplace", required=True, help="站点名称，例如 Amazon US")
    sellersprite.add_argument("--output", required=True, help="输出标准化 CSV 文件路径")
    sellersprite.set_defaults(handler=handle_sellersprite)

    # ---- 自动化采集 ----
    auto = subparsers.add_parser("auto-collect", help="自动化采集: ASIN发现 → Keepa历史 → Google Trends, 消耗完 token 截止")
    auto.add_argument("--domain", default="1", help="Amazon 站点: 数字(1=US) 或 'all' 遍历所有站点")
    auto.add_argument("--category", action="append", type=int, default=[], help="品类 ID, 可重复传入. 不指定则用默认热门品类")
    auto.add_argument("--search-term", action="append", default=[], help="搜索关键词, 可重复传入")
    auto.add_argument("--seed-file", help="ASIN 种子文件 CSV 路径")
    auto.add_argument("--db-path", help="DuckDB 文件路径 (默认使用 data_platform 下的)")
    auto.add_argument("--stale-hours", type=int, default=1440, help="未分层/非 P0-P2 ASIN 的兜底重采阈值小时数 (默认 1440，即 60 天；P0/P1/P2 按业务分层动态计算)")
    auto.add_argument("--batch-size", type=int, default=50, help="每批最大采集 ASIN 数 (默认 50, 实际按 token 余量动态调整)")
    auto.add_argument("--enable-trends", action="store_true", default=True, help="采集 Google Trends (默认开启)")
    auto.add_argument("--disable-trends", action="store_true", help="关闭 Google Trends 采集")
    auto.add_argument("--enable-strategy-expansion", action="store_true", help="启用下一阶段自动扩张: L2/L3/L4 shortlist + keyword 扩张")
    auto.add_argument("--strategy-pending-threshold", type=int, default=200, help="待采集池超过该阈值时跳过下一阶段扩张 (默认 200)")
    auto.add_argument("--strategy-category-limit", type=int, default=2, help="每轮最多扩张多少个 L2/L3/L4 shortlist 类目 (默认 2)")
    auto.add_argument("--strategy-keyword-limit", type=int, default=5, help="每轮最多扩张多少个 keyword (默认 5)")
    auto.add_argument("--strategy-category-cooldown-hours", type=int, default=24 * 30, help="同一 shortlist 类目再次扩张前的冷却小时数 (默认 720)")
    auto.add_argument("--strategy-keyword-cooldown-hours", type=int, default=72, help="同一 keyword 再次扩张前的冷却小时数 (默认 72)")
    auto.add_argument("--loop", action="store_true", help="持续运行: 每轮自动采集, 消耗完桶内 token")
    auto.add_argument("--interval-minutes", type=int, default=3, help="循环间隔分钟数 (默认 3, 配合 --loop)")
    auto.add_argument("--lock-file", default=str(DEFAULT_AUTO_COLLECT_LOCK_PATH), help="单实例锁文件路径")
    auto.set_defaults(handler=handle_auto_collect)

    score = subparsers.add_parser("refresh-business-priority", help="按业务评分规则重算 ASIN 分层与调度优先级")
    score.add_argument("--domain", type=int, default=1, help="Amazon 站点 (默认 1=US)")
    score.add_argument("--db-path", help="DuckDB 文件路径")
    score.add_argument("--asin", action="append", default=[], help="可选，只重算指定 ASIN，可重复传入")
    score.set_defaults(handler=handle_refresh_business_priority)

    backfill_raw = subparsers.add_parser(
        "backfill-product-raw",
        help="回填历史 product raw 文件: 压缩为 gzip, 生成 .meta.json, 写入 asin_raw_file_mapping",
    )
    backfill_raw.add_argument(
        "--products-dir",
        default=str(DEFAULT_PRODUCTS_DIR),
        help="product raw 目录 (默认使用 XIAMIMATE_RAW_PRODUCTS_DIR 或 data_platform/storage/raw/json/products)",
    )
    backfill_raw.add_argument("--db-path", help="DuckDB 文件路径")
    backfill_raw.add_argument("--limit", type=int, help="仅处理前 N 个 payload 文件")
    backfill_raw.add_argument("--rewrite-meta", action="store_true", help="即使已有 .meta.json 也重新生成")
    backfill_raw.add_argument("--keep-original", action="store_true", help="保留旧的 .json 文件，不删除")
    backfill_raw.add_argument("--apply", action="store_true", help="真正执行迁移；默认 dry-run")
    backfill_raw.set_defaults(handler=handle_backfill_product_raw)

    # ---- ASIN 发现 ----
    discover = subparsers.add_parser("discover-asins", help="通过 Keepa Bestsellers/搜索 发现 ASIN 并注册到 DuckDB")
    discover.add_argument("--domain", type=int, default=1, help="Amazon 站点 (1=US)")
    discover.add_argument("--category", action="append", type=int, default=[], help="品类 ID, 可重复传入")
    discover.add_argument("--search-term", action="append", default=[], help="搜索关键词, 可重复传入")
    discover.add_argument("--seed-file", help="ASIN 种子文件 CSV 路径")
    discover.add_argument("--db-path", help="DuckDB 文件路径")
    discover.add_argument("--output", help="可选, 同时输出 CSV")
    discover.set_defaults(handler=handle_discover_asins)

    # ---- 类目拉取 ----
    fetch_cats = subparsers.add_parser(
        "fetch-categories",
        help="从 Keepa 拉取 Amazon 品类树 (支持 --loop 循环拉取 L2/L3 子类目, 每节点 1 token)",
    )
    fetch_cats.add_argument("--domain", type=int, default=1, help="站点 ID (1=US)")
    fetch_cats.add_argument("--max-depth", type=int, default=3, help="递归深度 (默认 3, 拉取 L1→L2→L3)")
    fetch_cats.add_argument("--parent", type=int, default=0, help="起始品类节点 ID (单次模式)")
    fetch_cats.add_argument("--output", help="输出 CSV 路径")
    fetch_cats.add_argument("--cn-mapping", help="中文名映射 CSV 路径; 默认复用对应 amazon_{geo}_category_tree.csv")
    fetch_cats.add_argument("--db-path", help="DuckDB 文件路径")
    fetch_cats.add_argument(
        "--loop", action="store_true",
        help="循环模式: 逐个拉取 L1 类目的子类目树 (含 L2/L3, 每节点 1 token)",
    )
    fetch_cats.add_argument(
        "--interval-minutes", type=int, default=2,
        help="token 不足时等待的分钟数 (默认 2)",
    )
    fetch_cats.set_defaults(handler=handle_fetch_categories)

    # ---- DuckDB 状态 ----
    db_stats = subparsers.add_parser("db-stats", help="查看 DuckDB 中的数据统计")
    db_stats.add_argument("--db-path", help="DuckDB 文件路径")
    db_stats.add_argument("--domain", type=int, default=None, help="只看指定站点")
    db_stats.set_defaults(handler=handle_db_stats)

    # ---- Google Trends 特征工程 ----
    trends_feat = subparsers.add_parser("trends-features", help="将小时级 Google Trends 聚合为日级并计算衍生特征")
    trends_feat.add_argument("--input", action="append", required=True, help="小时级 Google Trends CSV, 可重复传入")
    trends_feat.add_argument("--keyword", help="只处理指定关键词")
    trends_feat.add_argument("--geo", help="只处理指定地域")
    trends_feat.add_argument("--agg", default="mean", choices=["mean", "median", "max", "sum"], help="日级聚合方式 (默认 mean)")
    trends_feat.add_argument("--output", required=True, help="输出 CSV 文件路径")
    trends_feat.set_defaults(handler=handle_trends_features)

    return parser


def handle_census(args: argparse.Namespace, settings) -> None:
    collector = USCensusCollector(api_key=settings.census_api_key, timeout=settings.request_timeout)
    rows = collector.fetch_imports(
        time=args.time,
        country_code=args.country_code,
        fields=[field.strip() for field in args.fields.split(",") if field.strip()],
        extra_params=parse_key_value_pairs(args.param),
    )
    output_path = resolve_output_path(settings.base_dir, args.output)
    write_rows("macro_trade_data", rows, output_path)
    print(f"Saved {len(rows)} rows to {output_path}")


def handle_eurostat(args: argparse.Namespace, settings) -> None:
    collector = EurostatCollector(settings.eurostat_base_url, timeout=settings.request_timeout)
    rows = collector.fetch_dataset(args.dataset, params=parse_key_value_pairs(args.param))
    output_path = resolve_output_path(settings.base_dir, args.output)
    write_rows("macro_trade_data", rows, output_path)
    print(f"Saved {len(rows)} rows to {output_path}")


def handle_comtrade(args: argparse.Namespace, settings) -> None:
    collector = UNComtradeCollector(
        settings.uncomtrade_base_url,
        api_key=settings.uncomtrade_api_key,
        timeout=settings.request_timeout,
    )
    rows = collector.fetch_trade_data(
        reporter_code=args.reporter_code,
        partner_code=args.partner_code,
        flow_code=args.flow_code,
        cmd_code=args.cmd_code,
        period=args.period,
        extra_params=parse_key_value_pairs(args.param),
    )
    output_path = resolve_output_path(settings.base_dir, args.output)
    write_rows("macro_trade_data", rows, output_path)
    print(f"Saved {len(rows)} rows to {output_path}")


def handle_google_trends(args: argparse.Namespace, settings) -> None:
    collector = GoogleTrendsCollector()
    rows = collector.fetch_interest_over_time(
        keywords=args.keyword,
        timeframe=args.timeframe,
        geo=args.geo,
        category=args.category,
        gprop=args.gprop,
    )
    output_path = resolve_output_path(settings.base_dir, args.output)
    write_rows("traffic_trend_data", rows, output_path)
    print(f"Saved {len(rows)} rows to {output_path}")


def handle_semrush(args: argparse.Namespace, settings) -> None:
    require_api_key(settings.semrush_api_key, "SEMRUSH_API_KEY")
    collector = SemrushCollector(settings.semrush_base_url, settings.semrush_api_key, timeout=settings.request_timeout)
    rows = collector.fetch_domain_history(
        domain=args.domain,
        database=args.database,
        report_type=args.report_type,
        display_limit=args.display_limit,
        export_columns=args.export_columns,
        display_daily=args.daily,
    )
    output_path = resolve_output_path(settings.base_dir, args.output)
    write_rows("traffic_trend_data", rows, output_path)
    print(f"Saved {len(rows)} rows to {output_path}")


def handle_ahrefs(args: argparse.Namespace, settings) -> None:
    require_api_key(settings.ahrefs_api_key, "AHREFS_API_KEY")
    collector = AhrefsCollector(settings.ahrefs_base_url, settings.ahrefs_api_key, timeout=settings.request_timeout)
    rows = collector.fetch_site_overview(
        target=args.target,
        country=args.country,
        mode=args.mode,
        endpoint_path=args.endpoint_path or settings.ahrefs_site_overview_path,
        extra_params=parse_key_value_pairs(args.param),
    )
    output_path = resolve_output_path(settings.base_dir, args.output)
    write_rows("traffic_trend_data", rows, output_path)
    print(f"Saved {len(rows)} rows to {output_path}")


def handle_keepa(args: argparse.Namespace, settings) -> None:
    require_api_key(settings.keepa_api_key, "KEEPA_API_KEY")
    collector = KeepaCollector(settings.keepa_base_url, settings.keepa_api_key, timeout=settings.request_timeout)
    rows, payload = collector.fetch_products(
        asins=args.asin,
        domain=args.domain,
        history=not args.no_history,
        stats_window_days=args.stats_window_days,
    )
    output_path = resolve_output_path(settings.base_dir, args.output)
    write_rows("product_tracking_data", rows, output_path)
    print(f"Saved {len(rows)} rows to {output_path}")
    if args.raw_output:
        raw_output_path = resolve_output_path(settings.base_dir, args.raw_output)
        write_json(payload, raw_output_path)
        print(f"Saved raw payload to {raw_output_path}")


def handle_keepa_history(args: argparse.Namespace, settings) -> None:
    require_api_key(settings.keepa_api_key, "KEEPA_API_KEY")
    collector = KeepaCollector(settings.keepa_base_url, settings.keepa_api_key, timeout=settings.request_timeout)
    rows, meta = collector.fetch_product_history(
        asins=args.asin,
        domain=args.domain,
        stats_window_days=args.stats_window_days,
    )
    output_path = resolve_output_path(settings.base_dir, args.output)
    write_rows("product_history_data", rows, output_path)
    print(f"Saved {len(rows)} history rows to {output_path}")
    print(f"Tokens remaining: {meta.get('tokens_left')}")
    if args.raw_output:
        raw_output_path = resolve_output_path(settings.base_dir, args.raw_output)
        write_json(meta["raw_products"], raw_output_path)
        print(f"Saved raw payload to {raw_output_path}")


def handle_keepa_tokens(args: argparse.Namespace, settings) -> None:
    require_api_key(settings.keepa_api_key, "KEEPA_API_KEY")
    collector = KeepaCollector(settings.keepa_base_url, settings.keepa_api_key, timeout=settings.request_timeout)
    status = collector.check_token_status()
    print(f"Tokens remaining: {status['tokens_left']}")
    print(f"Refill rate: {status['refill_rate']} tokens/min")
    print(f"Next refill in: {(status['refill_in_ms'] or 0) / 1000:.0f}s")


def handle_sellersprite(args: argparse.Namespace, settings) -> None:
    importer = SellerSpriteImporter()
    rows = importer.import_file(args.input, args.marketplace)
    output_path = resolve_output_path(settings.base_dir, args.output)
    write_rows("product_tracking_data", rows, output_path)
    print(f"Saved {len(rows)} rows to {output_path}")


def handle_auto_collect(args: argparse.Namespace, settings) -> None:
    require_api_key(settings.keepa_api_key, "KEEPA_API_KEY")
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from .scheduler import run_auto_collect, run_auto_collect_loop, run_multi_domain_collect_loop, ALL_DOMAINS

    lock_handle = acquire_process_lock(args.lock_file)

    try:
        # 解析 --domain: "all" 或数字
        is_all_domains = str(args.domain).lower() == "all"

        if is_all_domains:
            # 多站点采集模式
            if not args.loop:
                # 单次: 逐站点完成所有 L1 类目后切换
                for domain in ALL_DOMAINS:
                    from .collectors.product import KEEPA_DOMAIN_TO_GEO
                    geo = KEEPA_DOMAIN_TO_GEO.get(domain, "?")
                    print(f"\n=== Domain {domain}/{geo} ===")
                    domain_done = False
                    l1_round = 0
                    max_l1_rounds = 200  # 安全上限: 防止死循环
                    while not domain_done:
                        l1_round += 1
                        if l1_round > max_l1_rounds:
                            print(f"  Domain {domain}/{geo}: 达到安全上限 {max_l1_rounds} 轮, 跳过")
                            break
                        print(f"  L1 第 {l1_round} 次...")
                        stats = run_auto_collect(
                            keepa_api_key=settings.keepa_api_key,
                            keepa_base_url=settings.keepa_base_url,
                            db_path=args.db_path,
                            domain=domain,
                            enable_google_trends=args.enable_trends and not args.disable_trends,
                            enable_strategy_expansion=args.enable_strategy_expansion,
                            strategy_pending_threshold=args.strategy_pending_threshold,
                            strategy_category_limit=args.strategy_category_limit,
                            strategy_keyword_limit=args.strategy_keyword_limit,
                            strategy_category_cooldown_hours=args.strategy_category_cooldown_hours,
                            strategy_keyword_cooldown_hours=args.strategy_keyword_cooldown_hours,
                            stale_hours=args.stale_hours,
                            batch_size=args.batch_size,
                        )
                        _print_collect_stats(stats)
                        bs_pending = stats.get("bestseller_pending", 0)
                        asin_pending = stats.get("asins_pending", 0)
                        if bs_pending == 0 and asin_pending == 0:
                            print(f"  Domain {domain}/{geo}: 所有 L1 类目已完成")
                            domain_done = True
                        elif stats.get("asins_fetched", 0) == 0 and stats.get("asins_discovered", 0) == 0:
                            print(f"  Domain {domain}/{geo}: 无进展, 跳过")
                            domain_done = True
            else:
                # 循环: 每轮遍历所有 domain
                run_multi_domain_collect_loop(
                    interval_minutes=args.interval_minutes,
                    keepa_api_key=settings.keepa_api_key,
                    keepa_base_url=settings.keepa_base_url,
                    db_path=args.db_path,
                    seed_file=args.seed_file,
                    enable_google_trends=args.enable_trends and not args.disable_trends,
                    enable_strategy_expansion=args.enable_strategy_expansion,
                    strategy_pending_threshold=args.strategy_pending_threshold,
                    strategy_category_limit=args.strategy_category_limit,
                    strategy_keyword_limit=args.strategy_keyword_limit,
                    strategy_category_cooldown_hours=args.strategy_category_cooldown_hours,
                    strategy_keyword_cooldown_hours=args.strategy_keyword_cooldown_hours,
                    stale_hours=args.stale_hours,
                    batch_size=args.batch_size,
                )
            return

        # 单站点模式
        domain = int(args.domain)

        collect_kwargs = dict(
            keepa_api_key=settings.keepa_api_key,
            keepa_base_url=settings.keepa_base_url,
            db_path=args.db_path,
            domain=domain,
            categories=args.category or None,
            search_terms=args.search_term or None,
            seed_file=args.seed_file,
            enable_google_trends=args.enable_trends and not args.disable_trends,
            enable_strategy_expansion=args.enable_strategy_expansion,
            strategy_pending_threshold=args.strategy_pending_threshold,
            strategy_category_limit=args.strategy_category_limit,
            strategy_keyword_limit=args.strategy_keyword_limit,
            strategy_category_cooldown_hours=args.strategy_category_cooldown_hours,
            strategy_keyword_cooldown_hours=args.strategy_keyword_cooldown_hours,
            stale_hours=args.stale_hours,
            batch_size=args.batch_size,
        )

        if args.loop:
            run_auto_collect_loop(
                interval_minutes=args.interval_minutes,
                **collect_kwargs,
            )
            return

        stats = run_auto_collect(**collect_kwargs)
        _print_collect_stats(stats)
    finally:
        lock_handle.close()


def handle_refresh_business_priority(args: argparse.Namespace, settings) -> None:
    from .business_scoring import refresh_domain_business_priorities
    from .storage import DuckDBStorage

    with DuckDBStorage(args.db_path) as storage:
        result = refresh_domain_business_priorities(
            storage,
            domain=args.domain,
            asins=args.asin or None,
        )

    print(
        f"Scored {result.get('scored', 0)} ASIN(s) for domain {args.domain}; "
        f"tiers={result.get('tiers', {})}"
    )


def handle_backfill_product_raw(args: argparse.Namespace, settings) -> None:
    from .backfill_product_raw_archives import backfill_product_raw_archives

    result = backfill_product_raw_archives(
        products_dir=Path(args.products_dir),
        duckdb_path=Path(args.db_path).resolve() if args.db_path else None,
        apply=args.apply,
        limit=args.limit,
        rewrite_meta=args.rewrite_meta,
        keep_original=args.keep_original,
    )

    print(f"mode: {'apply' if args.apply else 'dry-run'}")
    print(f"scanned_files: {result['scanned_files']}")
    print(f"compressed_files: {result['compressed_files']}")
    print(f"meta_written: {result['meta_written']}")
    print(f"mapping_rows_written: {result['mapping_rows_written']}")
    print(f"collection_logs_updated: {result['collection_logs_updated']}")
    print(f"original_json_removed: {result['original_json_removed']}")
    print(f"skipped_files: {result['skipped_files']}")
    if result.get("errors"):
        print(f"errors: {len(result['errors'])}")
        for err in result["errors"][:20]:
            print(f"  - {err}")


def _print_collect_stats(stats: dict) -> None:
    print(f"\n完成: 发现 {stats['asins_discovered']} ASIN, "
          f"采集 {stats['asins_fetched']} ASIN, "
          f"写入 {stats['history_rows_ingested']} 行, "
          f"消耗 {stats['tokens_consumed']} token, "
          f"耗时 {stats['duration_seconds']}s")
    if stats.get("business_scores_updated"):
        print(
            f"业务评分: 更新 {stats['business_scores_updated']} 个 ASIN, "
            f"分层 {stats.get('business_tier_stats', {})}"
        )
    if stats.get("strategy_categories_selected") or stats.get("strategy_keywords_selected"):
        print(
            f"下一阶段扩张: 类目 {stats.get('strategy_categories_selected', 0)} 个, "
            f"keyword {stats.get('strategy_keywords_selected', 0)} 个, "
            f"新增 ASIN {stats.get('strategy_asins_discovered', 0)} 个"
        )
    if stats.get("errors"):
        print(f"错误: {len(stats['errors'])} 个")
        for err in stats["errors"]:
            print(f"  - {err}")


def handle_discover_asins(args: argparse.Namespace, settings) -> None:
    require_api_key(settings.keepa_api_key, "KEEPA_API_KEY")
    from .asin_discovery import KeepaAsinDiscovery, load_seed_asins, save_discovered_asins
    from .storage import DuckDBStorage

    discovery = KeepaAsinDiscovery(
        base_url=settings.keepa_base_url,
        api_key=settings.keepa_api_key,
    )
    all_discovered: list[dict] = []

    # 种子文件
    if args.seed_file:
        seeds = load_seed_asins(args.seed_file)
        for s in seeds:
            s.setdefault("discovery_source", "seed")
            s.setdefault("domain", args.domain)
        all_discovered.extend(seeds)
        print(f"从种子文件加载 {len(seeds)} 个 ASIN")

    # Bestsellers
    for cat_id in (args.category or []):
        try:
            asins = discovery.fetch_best_sellers(category=cat_id, domain=args.domain)
            for asin in asins:
                all_discovered.append({
                    "asin": asin, "domain": args.domain,
                    "category_id": cat_id, "discovery_source": "bestseller",
                })
            print(f"品类 {cat_id}: {len(asins)} 个 bestseller")
        except Exception as e:
            print(f"品类 {cat_id} 失败: {e}")

    # 搜索
    for term in (args.search_term or []):
        try:
            asins = discovery.search_products(term=term, domain=args.domain)
            for asin in asins:
                all_discovered.append({
                    "asin": asin, "domain": args.domain,
                    "search_term": term, "discovery_source": "search",
                })
            print(f"搜索 '{term}': {len(asins)} 个 ASIN")
        except Exception as e:
            print(f"搜索 '{term}' 失败: {e}")

    # 注册到 DuckDB
    if all_discovered:
        with DuckDBStorage(args.db_path) as db:
            new_count = db.register_asins(all_discovered)
            stats = db.get_registry_stats(args.domain)
        print(f"新注册 {new_count} 个 ASIN (总计 {stats['total_asins']})")
    else:
        print("未发现任何 ASIN")

    # 可选 CSV 输出
    if args.output and all_discovered:
        output_path = resolve_output_path(settings.base_dir, args.output)
        save_discovered_asins(all_discovered, output_path)
        print(f"Saved {len(all_discovered)} ASINs to {output_path}")


def handle_fetch_categories(args: argparse.Namespace, settings) -> None:
    require_api_key(settings.keepa_api_key, "KEEPA_API_KEY")
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from .fetch_categories import run_category_fetch_loop, fetch_category_tree, flatten_categories, load_cn_mapping_for_domain, FIELDNAMES, _save_raw_categories
    import csv as csv_mod

    if args.loop:
        run_category_fetch_loop(
            api_key=settings.keepa_api_key,
            domain=args.domain,
            max_depth=args.max_depth,
            interval_minutes=args.interval_minutes,
            output_csv=args.output,
            cn_mapping_path=args.cn_mapping,
            db_path=args.db_path,
        )
        return

    # 单次模式
    print(f"正在从 Keepa 拉取品类树 (domain={args.domain}, parent={args.parent})...")
    categories = fetch_category_tree(
        settings.keepa_api_key, domain=args.domain,
        parent=args.parent, max_depth=args.max_depth,
    )
    print(f"获取到 {len(categories)} 个品类节点")
    _save_raw_categories(categories, args.parent, args.domain)

    cn_mapping, cn_mapping_source = load_cn_mapping_for_domain(args.domain, args.cn_mapping)
    if cn_mapping:
        print(f"已从 {cn_mapping_source} 加载 {len(cn_mapping)} 条中文名映射")
    rows = flatten_categories(categories, max_depth=args.max_depth, cn_mapping=cn_mapping)
    print(f"展平后 {len(rows)} 行 (max_depth={args.max_depth})")

    if args.output:
        path = Path(args.output).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv_mod.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)
        print(f"已保存到 {path}")
    else:
        for row in rows[:50]:
            cn = f" ({row['category_cn']})" if row["category_cn"] else ""
            print(
                f"{row['category_id']:>15}  {row['level']}  "
                f"{row['category_en']:<50}{cn}  ({row['product_count']:,} products)"
            )
        if len(rows) > 50:
            print(f"... 还有 {len(rows) - 50} 行, 请用 --output 导出查看")


def handle_db_stats(args: argparse.Namespace, settings) -> None:
    from .storage import DuckDBStorage
    with DuckDBStorage(args.db_path) as db:
        stats = db.get_registry_stats(args.domain)
    print(f"ASIN 注册表:")
    print(f"  总计: {stats['total_asins']}")
    print(f"  已采集: {stats['fetched']}")
    print(f"  未采集: {stats['never_fetched']}")
    print(f"历史数据行: {stats['history_rows']}")
    print(f"Trends 数据行: {stats['trends_rows']}")


def handle_trends_features(args: argparse.Namespace, settings) -> None:
    from ..sales_forecast.trends_features import process_trends_csv
    input_files = [resolve_output_path(settings.base_dir, p) for p in args.input]
    output_path = resolve_output_path(settings.base_dir, args.output)
    df = process_trends_csv(
        input_files,
        output_path=output_path,
        agg=args.agg,
        keyword=args.keyword,
        geo=args.geo,
    )
    print(f"日级 Trends 特征: {len(df)} 行, {len(df.columns)} 列")
    print(f"  列: {list(df.columns)}")
    print(f"Saved to {output_path}")


def parse_key_value_pairs(pairs: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise SystemExit(f"Invalid --param value: {pair}. Expected key=value")
        key, value = pair.split("=", 1)
        result[key] = value
    return result


def require_api_key(value: str | None, env_name: str) -> None:
    if not value:
        raise SystemExit(f"Missing API key: please set {env_name}")


def resolve_output_path(base_dir: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    if path.parts and path.parts[0] == base_dir.name:
        path = Path(*path.parts[1:])
    return (base_dir / path).resolve()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
