from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_platform.product_query_assistant import normalize_product_recall_query


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize a product recall query using the product recall assistant.")
    parser.add_argument("product_query", help="Raw product query")
    parser.add_argument("--marketplace", default="US", help="Marketplace code, default US")
    parser.add_argument("--query-alias", action="append", default=[], help="Optional query alias, repeatable")
    parser.add_argument("--category-hint", action="append", default=[], help="Optional category hint, repeatable")
    parser.add_argument("--prefix", default="THEME_QUERY_NORMALIZER", help="LLM env prefix")
    parser.add_argument("--profile", choices=("minimax", "deepseek"), help="Temporarily use a named provider profile")
    parser.add_argument("--timeout-seconds", type=float, help="Override provider timeout for this run only")
    args = parser.parse_args()

    if args.profile is not None:
        os.environ[f"{args.prefix}_PROFILE"] = args.profile
    if args.timeout_seconds is not None:
        os.environ[f"{args.prefix}_TIMEOUT_SECONDS"] = str(args.timeout_seconds)

    result = normalize_product_recall_query(
        product_query=args.product_query,
        query_aliases=args.query_alias,
        category_hints=args.category_hint,
        marketplace=args.marketplace,
        env_prefix=args.prefix,
    )
    print(json.dumps(result.__dict__, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()