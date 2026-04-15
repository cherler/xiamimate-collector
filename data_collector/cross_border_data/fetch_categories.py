"""从 Keepa API 拉取 Amazon 完整品类树并输出 CSV.

Keepa category API 每次请求消耗 1 token.
每个品类节点包含: catId, name, children[], productCount 等.

用法:
    # 拉取品类树 (默认复用 doc/amazon_us_category_tree.csv 中已有中文名)
    python -m data_collector.cross_border_data.fetch_categories \
        --domain 1 --max-depth 2 \
        --output doc/amazon_us_category_tree.csv

    # 如需显式覆盖映射源, 也应传入已有的 *_tree.csv
    python -m data_collector.cross_border_data.fetch_categories \
        --domain 1 --max-depth 2 \
        --cn-mapping doc/amazon_us_category_tree.csv \
        --output doc/amazon_us_category_tree.csv

    # 自动循环拉取 L2/L3 子类目 (每 50 分钟一次, 每次消耗 ~1 token)
    python -m data_collector.cross_border_data.fetch_categories \
        --domain 1 --max-depth 3 --loop --interval-minutes 50 \
        --output doc/amazon_us_category_tree_full.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests as _requests

from .config import load_settings
from .collectors.base import BaseCollector
from .collectors.product import KEEPA_DOMAIN_TO_GEO

logger = logging.getLogger(__name__)


class _Client(BaseCollector):
    pass

_RAW_DIR = Path(__file__).resolve().parents[2] / "data_platform" / "storage" / "raw" / "json" / "categories"
_DOC_DIR = Path(__file__).resolve().parents[2] / "doc"


def _get_tree_csv_path_for_domain(domain: int) -> Path:
    geo = KEEPA_DOMAIN_TO_GEO.get(domain, "US").lower()
    return _DOC_DIR / f"amazon_{geo}_category_tree.csv"


def resolve_cn_mapping_path(domain: int, path: str | Path | None = None) -> Path | None:
    if path:
        return Path(path).expanduser().resolve()
    default_path = _get_tree_csv_path_for_domain(domain)
    return default_path if default_path.exists() else None


def fetch_category_tree(
    api_key: str,
    domain: int = 1,
    parent: int = 0,
    max_depth: int = 1,
) -> dict:
    """调用 Keepa category API 获取品类树 (递归拉取子品类).

    category=0 返回所有根节点; 其他值返回单个节点.
    需要递归请求子节点以获得完整树.
    内置 429 限流重试 + 请求间隔.

    Returns
    -------
    dict
        {catId: {name, children, productCount, ...}, ...}
    """
    all_categories: dict = {}

    def _fetch_one(cat_id: int) -> dict:
        url = "https://api.keepa.com/category"
        # parents=0: 不请求 parent tree, 每次调用只消耗 1 token (而非 2)
        params = {"key": api_key, "domain": domain, "category": cat_id, "parents": 0}
        for attempt in range(5):
            resp = _requests.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
                wait = (body.get("refillIn", 60000) / 1000) + 1
                print(f"    429 限流, 等待 {wait:.0f}s (attempt {attempt + 1})...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json().get("categories", {})
        return {}

    # 第一次请求获取根节点
    root_cats = _fetch_one(parent)
    all_categories.update(root_cats)

    if max_depth <= 1:
        return all_categories

    # 逐层递归拉取子节点
    current_layer_ids: list[int] = []
    for node in root_cats.values():
        current_layer_ids.extend(node.get("children") or [])

    for depth in range(2, max_depth + 1):
        if not current_layer_ids:
            break
        next_layer_ids: list[int] = []
        total = len(current_layer_ids)
        print(f"  拉取第 {depth} 层: {total} 个子品类...")
        for i, child_id in enumerate(current_layer_ids):
            if str(child_id) in all_categories:
                continue
            child_cats = _fetch_one(child_id)
            all_categories.update(child_cats)
            for cn in child_cats.values():
                next_layer_ids.extend(cn.get("children") or [])
            if (i + 1) % 20 == 0:
                print(f"    进度: {i + 1}/{total}")
            time.sleep(1.5)  # 避免触发限流
        current_layer_ids = next_layer_ids

    return all_categories


def load_cn_mapping(path: str | Path) -> dict[str, str]:
    """从类目树 CSV 加载英文名 → 中文名的映射.

    兼容任何包含 category_en, category_cn 两列的 CSV.
    """
    mapping: dict[str, str] = {}
    p = Path(path)
    if not p.exists():
        return mapping
    with p.open(encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            en = (row.get("category_en") or "").strip()
            cn = (row.get("category_cn") or "").strip()
            if en and cn:
                mapping[en] = cn
    return mapping


def load_cn_mapping_for_domain(
    domain: int,
    path: str | Path | None = None,
) -> tuple[dict[str, str], Path | None]:
    resolved_path = resolve_cn_mapping_path(domain, path)
    if resolved_path is None:
        return {}, None
    return load_cn_mapping(resolved_path), resolved_path


def flatten_categories(
    categories: dict,
    max_depth: int = 99,
    cn_mapping: dict[str, str] | None = None,
) -> list[dict]:
    """将品类树展平为行列表.

    Returns
    -------
    list[dict]
        每行: category_id, category_en, category_cn, parent_id, level, product_count
    """
    cn_map = cn_mapping or {}
    rows: list[dict] = []
    visited: set[int] = set()

    def _walk(cat_id: int | str, depth: int) -> None:
        cat_id_int = int(cat_id)
        if cat_id_int in visited or depth > max_depth:
            return
        visited.add(cat_id_int)

        node = categories.get(str(cat_id)) or categories.get(cat_id)
        if node is None:
            return

        name = node.get("name", "")
        parent_id = node.get("parent", 0)
        rows.append({
            "category_id": cat_id_int,
            "category_en": name,
            "category_cn": cn_map.get(name, ""),
            "parent_id": parent_id if parent_id else "",
            "level": f"L{depth + 1}",
            "depth": depth + 1,
            "product_count": node.get("productCount", 0),
        })

        for child_id in (node.get("children") or []):
            _walk(child_id, depth + 1)

    # 从根节点开始遍历
    for cat_id, node in categories.items():
        parent = node.get("parent")
        if parent == 0 or parent is None:
            _walk(cat_id, 0)

    # 补充未被遍历到的节点
    for cat_id in categories:
        if int(cat_id) not in visited:
            _walk(cat_id, 0)

    rows.sort(key=lambda r: (r["level"], -r["product_count"]))
    return rows


FIELDNAMES = ["category_id", "category_en", "category_cn", "parent_id", "level", "depth", "product_count"]


def _save_raw_categories(categories: dict, parent_id: int, domain: int) -> Path | None:
    """保存原始类目 API 响应到本地 JSON."""
    try:
        _RAW_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = _RAW_DIR / f"cat_{parent_id}_domain{domain}_{ts}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(categories, f, ensure_ascii=False, default=str)
        return path
    except Exception as e:
        logger.warning(f"保存原始类目响应失败: {e}")
        return None


def run_category_fetch_loop(
    *,
    api_key: str,
    domain: int = 1,
    max_depth: int = 3,
    interval_minutes: int = 2,
    output_csv: str | Path | None = None,
    cn_mapping_path: str | Path | None = None,
    db_path: str | Path | None = None,
) -> None:
    """循环拉取 L1 类目的 L3 子类目.

    每轮从 DuckDB category_registry 中取一个 children_fetched_at IS NULL 的 L1 类目,
    递归拉取 L1→L2→L3 子类目, 所有层级全部存入 DuckDB.

    Token 策略:
    - Category Lookup: 1 token / 节点 (需递归拉取子节点)
    - 每个 L1 的 token 消耗 = 1 + L2数量 + L3数量
    - 外层只在 token=0 时等待 interval_minutes 分钟
    """
    from .storage import DuckDBStorage
    from .collectors.product import KeepaCollector

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    storage = DuckDBStorage(db_path)
    token_checker = KeepaCollector(
        base_url="https://api.keepa.com/product",
        api_key=api_key,
    )
    cn_mapping, cn_mapping_source = load_cn_mapping_for_domain(domain, cn_mapping_path)
    if cn_mapping_source and cn_mapping:
        logger.info("中文名映射来源: %s (%s 条)", cn_mapping_source, len(cn_mapping))

    round_num = 0
    total_cats = 0

    logger.info(
        f"=== 进入类目拉取循环模式 === token 充足时连续拉取, 不足时等待 {interval_minutes} 分钟, Ctrl-C 退出"
    )

    try:
        while True:
            # 先检查是否还有待拉取的 L1
            next_l1 = storage.get_next_l1_for_children_fetch(domain)
            if next_l1 is None:
                logger.info("所有 L1 类目的子类目均已拉取完成, 退出循环")
                break

            # 查 token 余额
            try:
                token_info = token_checker.check_token_status()
                tokens_left = token_info.get("tokens_left", 0)
                logger.info(f"当前 token 余量: {tokens_left}")
            except Exception as e:
                logger.warning(f"查询 token 失败: {e}, 假设为 0")
                tokens_left = 0

            if tokens_left < 1:
                logger.info(
                    f"token 不足 ({tokens_left}), 等待 {interval_minutes} 分钟后重试..."
                )
                time.sleep(interval_minutes * 60)
                continue

            round_num += 1
            cat_id = next_l1["category_id"]
            cat_name = next_l1.get("category_en") or str(cat_id)

            try:
                logger.info(
                    f"--- 第 {round_num} 轮: 拉取 {cat_id} ({cat_name}) 的子类目 "
                    f"(max_depth={max_depth}), token={tokens_left} ---"
                )

                # 调用 Keepa category API (1 次调用 = 1 token, 返回完整子树)
                categories = fetch_category_tree(
                    api_key, domain=domain, parent=cat_id, max_depth=max_depth,
                )
                logger.info(f"获取到 {len(categories)} 个品类节点")

                # 保存原始响应
                _save_raw_categories(categories, cat_id, domain)

                # 展平 (max_depth=99 展平全部, 因为 API 已返回完整子树)
                rows = flatten_categories(
                    categories, max_depth=99, cn_mapping=cn_mapping,
                )

                logger.info(f"展平后 {len(rows)} 个类目 (含所有层级)")

                if rows:
                    cat_dicts = []
                    for r in rows:
                        depth = int(r["level"][1]) if r["level"].startswith("L") else 1
                        cat_dicts.append({
                            "category_id": r["category_id"],
                            "category_en": r["category_en"],
                            "category_cn": r.get("category_cn"),
                            "parent_id": r["parent_id"] if r["parent_id"] != "" else None,
                            "level": r["level"],
                            "depth": depth,
                            "product_count": r["product_count"],
                        })
                    new_count = storage.upsert_categories_from_tree(cat_dicts, domain)
                    total_cats += len(rows)
                    logger.info(
                        f"写入 {len(rows)} 个类目 (新增 {new_count}) "
                        f"到 category_registry"
                    )

                # 追加到 CSV (附带输出, 核心状态在 DuckDB)
                if output_csv:
                    csv_path = Path(output_csv).resolve()
                    csv_path.parent.mkdir(parents=True, exist_ok=True)
                    write_header = not csv_path.exists() or csv_path.stat().st_size == 0
                    with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
                        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                        if write_header:
                            writer.writeheader()
                        for r in rows:
                            r.setdefault("depth", int(r["level"][1]) if r["level"].startswith("L") else 1)
                        writer.writerows(rows)
                    logger.info(f"追加 {len(rows)} 行到 {csv_path}")

                # 标记该 L1 已完成子类目拉取 (DuckDB category_registry.children_fetched_at)
                storage.mark_category_children_fetched(cat_id, domain)
                logger.info(f"类目 {cat_id} ({cat_name}) 子类目拉取完成, 已标记 children_fetched_at")

            except Exception as e:
                logger.error(f"第 {round_num} 轮出错 (不退出, 等待下一轮): {e}", exc_info=True)

            # 拉取成功后不等待, 立即检查下一个 (循环开头会再查 token)

    except KeyboardInterrupt:
        logger.info(
            f"\n=== 类目拉取已停止 === "
            f"共 {round_num} 轮, 拉取 {total_cats} 个子类目"
        )
    finally:
        storage.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="从 Keepa 拉取 Amazon 品类树")
    parser.add_argument("--domain", type=int, default=1, help="站点 ID (1=US)")
    parser.add_argument("--max-depth", type=int, default=3, help="递归深度 (默认 3, 拉取 L1→L2→L3)")
    parser.add_argument("--output", type=str, default=None, help="输出 CSV 路径")
    parser.add_argument("--parent", type=int, default=0, help="起始品类节点 ID")
    parser.add_argument(
        "--cn-mapping", type=str, default=None,
        help="中文名映射 CSV 路径; 默认复用对应 amazon_{geo}_category_tree.csv",
    )
    parser.add_argument("--db-path", type=str, default=None, help="DuckDB 文件路径")
    parser.add_argument(
        "--loop", action="store_true",
        help="循环模式: 逐个拉取 L1 类目的子类目树 (含 L2/L3, 每节点 1 token)",
    )
    parser.add_argument(
        "--interval-minutes", type=int, default=2,
        help="token 不足时等待的分钟数 (默认 2). Category API 1 token/类目, 内部已有 429 自动等待",
    )
    args = parser.parse_args()

    api_key = load_settings().keepa_api_key
    if not api_key:
        print("错误: 未设置 KEEPA_API_KEY 环境变量", file=sys.stderr)
        sys.exit(1)

    # 循环模式: 从 DuckDB 逐个拉取 L1 类目的子类目
    if args.loop:
        run_category_fetch_loop(
            api_key=api_key,
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
        api_key, domain=args.domain, parent=args.parent, max_depth=args.max_depth,
    )
    print(f"获取到 {len(categories)} 个品类节点")

    # 保存原始响应
    _save_raw_categories(categories, args.parent, args.domain)

    cn_mapping, cn_mapping_source = load_cn_mapping_for_domain(args.domain, args.cn_mapping)
    if cn_mapping:
        source_label = cn_mapping_source if cn_mapping_source else "内存映射"
        print(f"已从 {source_label} 加载 {len(cn_mapping)} 条中文名映射")

    rows = flatten_categories(categories, max_depth=args.max_depth, cn_mapping=cn_mapping)
    print(f"展平后 {len(rows)} 行 (max_depth={args.max_depth})")

    if args.output:
        path = Path(args.output).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)
        print(f"已保存到 {path}")
    else:
        for row in rows[:50]:
            cn = f" ({row['category_cn']})" if row["category_cn"] else ""
            print(
                f"{row['category_id']:>15}  "
                f"{row['level']}  "
                f"{row['category_en']:<50}{cn}  "
                f"({row['product_count']:,} products)"
            )
        if len(rows) > 50:
            print(f"... 还有 {len(rows) - 50} 行, 请用 --output 导出查看")


if __name__ == "__main__":
    main()
