from __future__ import annotations

import argparse
import gzip
import json
import os
import re
from pathlib import Path
from typing import Any

from .storage import DuckDBStorage


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PRODUCTS_DIR = Path(
    os.environ.get(
        "XIAMIMATE_RAW_PRODUCTS_DIR",
        PROJECT_ROOT / "data_platform" / "storage" / "raw" / "json" / "products",
    )
).expanduser().resolve()
_TIMESTAMP_RE = re.compile(r"_(\d{8})_(\d{6})$")


def _iter_payload_files(products_dir: Path) -> list[Path]:
    payload_files: list[Path] = []
    for path in sorted(products_dir.iterdir()):
        if not path.is_file() or path.name.endswith(".meta.json"):
            continue
        if path.name.endswith(".json") or path.name.endswith(".json.gz"):
            payload_files.append(path)
    return payload_files


def _read_payload(path: Path) -> dict[str, Any] | list[Any]:
    if path.name.endswith(".json.gz"):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            return json.load(handle)
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_payload_gzip(payload: dict[str, Any] | list[Any], target_path: Path) -> None:
    with gzip.open(target_path, "wt", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, default=str)


def _base_name(path: Path) -> str:
    if path.name.endswith(".json.gz"):
        return path.name[: -len(".json.gz")]
    if path.name.endswith(".json"):
        return path.name[: -len(".json")]
    return path.stem


def _meta_path_for(path: Path) -> Path:
    return path.parent / f"{_base_name(path)}.meta.json"


def _gzip_path_for(path: Path) -> Path:
    if path.name.endswith(".json.gz"):
        return path
    return path.with_suffix(path.suffix + ".gz")


def _infer_saved_at(path: Path) -> str | None:
    match = _TIMESTAMP_RE.search(_base_name(path))
    if not match:
        return None
    date_part = match.group(1)
    time_part = match.group(2)
    return (
        f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]} "
        f"{time_part[:2]}:{time_part[2:4]}:{time_part[4:6]}"
    )


def _extract_products(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        products = payload.get("products")
        if isinstance(products, list):
            return [item for item in products if isinstance(item, dict)]
        raw_products = payload.get("raw_products")
        if isinstance(raw_products, dict):
            nested = raw_products.get("products")
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]
    return []


def _infer_domain(products: list[dict[str, Any]], fallback_domain: int | None) -> int | None:
    if fallback_domain is not None:
        return int(fallback_domain)
    domains = sorted(
        {
            int(product.get("domainId"))
            for product in products
            if product.get("domainId") not in (None, "")
        }
    )
    if not domains:
        return None
    return domains[0]


def _extract_asins(products: list[dict[str, Any]]) -> list[str]:
    return sorted(
        {
            str(product.get("asin", "")).strip()
            for product in products
            if str(product.get("asin", "")).strip()
        }
    )


def _load_collection_log_context(storage: DuckDBStorage, products_dir: Path) -> dict[str, dict[str, Any]]:
    rows = storage.conn.execute(
        """SELECT raw_file_path, source, domain
           FROM curated.collection_log
           WHERE raw_file_path IS NOT NULL
             AND raw_file_path LIKE ?""",
        [f"{str(products_dir.resolve())}%"],
    ).fetchall()
    return {
        str(raw_file_path): {"source": source, "domain": domain}
        for raw_file_path, source, domain in rows
        if raw_file_path
    }


def _update_collection_log_path(storage: DuckDBStorage, old_path: str, new_path: str) -> int:
    matched = _count_collection_log_path(storage, old_path)
    if matched:
        storage.conn.execute(
            "UPDATE curated.collection_log SET raw_file_path = ? WHERE raw_file_path = ?",
            [new_path, old_path],
        )
    return int(matched or 0)


def _count_collection_log_path(storage: DuckDBStorage, raw_path: str) -> int:
    matched = storage.conn.execute(
        "SELECT COUNT(*) FROM curated.collection_log WHERE raw_file_path = ?",
        [raw_path],
    ).fetchone()[0]
    return int(matched or 0)


def _build_meta_payload(
    *,
    payload_path: Path,
    gzip_path: Path,
    domain: int | None,
    asins: list[str],
) -> dict[str, Any]:
    saved_at = _infer_saved_at(payload_path)
    label = _base_name(payload_path)
    if saved_at is None:
        saved_at = ""
    return {
        "category": "products",
        "label": label,
        "saved_at": saved_at,
        "domain": domain,
        "asin_count": len(asins),
        "asins": asins,
        "json_file": gzip_path.name,
        "compression": "gzip",
        "is_compressed": True,
    }


def backfill_product_raw_archives(
    *,
    products_dir: Path = DEFAULT_PRODUCTS_DIR,
    duckdb_path: Path | None = None,
    apply: bool = False,
    limit: int | None = None,
    rewrite_meta: bool = False,
    keep_original: bool = False,
) -> dict[str, Any]:
    products_dir = products_dir.resolve()
    files = _iter_payload_files(products_dir)
    if limit is not None:
        files = files[:limit]

    result: dict[str, Any] = {
        "scanned_files": len(files),
        "compressed_files": 0,
        "meta_written": 0,
        "mapping_rows_written": 0,
        "collection_logs_updated": 0,
        "original_json_removed": 0,
        "skipped_files": 0,
        "errors": [],
        "apply": apply,
    }

    with DuckDBStorage(duckdb_path) as storage:
        log_context = _load_collection_log_context(storage, products_dir)

        for payload_path in files:
            gzip_path = _gzip_path_for(payload_path)
            meta_path = _meta_path_for(payload_path)
            payload_is_legacy_json = payload_path.name.endswith(".json") and not payload_path.name.endswith(".meta.json")

            try:
                payload = _read_payload(payload_path)
                products = _extract_products(payload)
                context = log_context.get(str(payload_path)) or log_context.get(str(gzip_path)) or {}
                asins = _extract_asins(products)
                domain = _infer_domain(products, context.get("domain"))
                source = context.get("source") or "auto_collect"

                needs_compress = payload_is_legacy_json and not gzip_path.exists()
                needs_meta = rewrite_meta or (not meta_path.exists())
                if not asins:
                    result["skipped_files"] += 1
                    continue

                if not apply:
                    if needs_compress:
                        result["compressed_files"] += 1
                    if needs_meta:
                        result["meta_written"] += 1
                    if domain is not None:
                        result["mapping_rows_written"] += len(asins)
                    if payload_is_legacy_json:
                        result["collection_logs_updated"] += _count_collection_log_path(
                            storage,
                            str(payload_path),
                        )
                        if not keep_original:
                            result["original_json_removed"] += 1
                    continue

                if needs_compress:
                    _write_payload_gzip(payload, gzip_path)
                    result["compressed_files"] += 1

                if needs_meta:
                    meta_payload = _build_meta_payload(
                        payload_path=payload_path,
                        gzip_path=gzip_path,
                        domain=domain,
                        asins=asins,
                    )
                    with meta_path.open("w", encoding="utf-8") as handle:
                        json.dump(meta_payload, handle, ensure_ascii=False, indent=2)
                    result["meta_written"] += 1

                if domain is None:
                    result["errors"].append(
                        f"{payload_path.name}: cannot infer domain for asin_raw_file_mapping"
                    )
                else:
                    result["mapping_rows_written"] += storage.upsert_asin_raw_file_mappings(
                        asins=asins,
                        domain=domain,
                        source=source,
                        raw_file_path=gzip_path,
                    )

                if payload_is_legacy_json:
                    result["collection_logs_updated"] += _update_collection_log_path(
                        storage,
                        str(payload_path),
                        str(gzip_path),
                    )
                    if not keep_original and gzip_path.exists() and payload_path.exists():
                        payload_path.unlink()
                        result["original_json_removed"] += 1
            except Exception as exc:
                result["errors"].append(f"{payload_path.name}: {exc}")

    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill legacy product raw JSON files into gzip + meta + asin_raw_file_mapping.",
    )
    parser.add_argument(
        "--products-dir",
        default=str(DEFAULT_PRODUCTS_DIR),
        help="Product raw directory. Default: data_platform/storage/raw/json/products",
    )
    parser.add_argument("--duckdb-path", help="Optional DuckDB path override.")
    parser.add_argument("--limit", type=int, help="Only process the first N payload files.")
    parser.add_argument("--rewrite-meta", action="store_true", help="Rewrite meta sidecars even when they already exist.")
    parser.add_argument("--keep-original", action="store_true", help="Keep legacy .json files after .json.gz is created.")
    parser.add_argument("--apply", action="store_true", help="Apply changes. Without this flag the command runs in dry-run mode.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    result = backfill_product_raw_archives(
        products_dir=Path(args.products_dir),
        duckdb_path=Path(args.duckdb_path).resolve() if args.duckdb_path else None,
        apply=args.apply,
        limit=args.limit,
        rewrite_meta=args.rewrite_meta,
        keep_original=args.keep_original,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()