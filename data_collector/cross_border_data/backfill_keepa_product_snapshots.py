from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from .collectors.product import normalize_keepa_product_snapshot
from .storage import DuckDBStorage
from .utils import write_rows


_TIMESTAMP_RE = re.compile(r"_(\d{8})_(\d{6})$")


def _infer_update_time(path: Path) -> str:
    match = _TIMESTAMP_RE.search(path.stem)
    if not match:
        raise ValueError(f"Cannot infer timestamp from file name: {path.name}")
    return f"{match.group(1)[:4]}-{match.group(1)[4:6]}-{match.group(1)[6:8]} {match.group(2)[:2]}:{match.group(2)[2:4]}:{match.group(2)[4:6]}"


def _iter_input_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    return sorted(file for file in path.glob("*.json") if file.is_file())


def backfill_keepa_product_snapshots(
    *,
    input_path: Path,
    output_dir: Path,
    domain: int,
    duckdb_path: Path | None = None,
) -> tuple[int, int]:
    files = _iter_input_files(input_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    file_count = 0
    row_count = 0
    storage = DuckDBStorage(duckdb_path) if duckdb_path else None

    try:
        for file_path in files:
            with file_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)

            update_time = _infer_update_time(file_path)
            products = payload.get("products") or []
            rows = [
                normalize_keepa_product_snapshot(
                    product,
                    domain=domain,
                    update_time=update_time,
                    source_url=str(file_path.resolve()),
                )
                for product in products
            ]

            output_path = output_dir / f"{file_path.stem}.csv"
            write_rows("product_tracking_data", rows, output_path)
            if storage is not None:
                storage.ingest_keepa_product_snapshots(rows, domain=domain)
            file_count += 1
            row_count += len(rows)
    finally:
        if storage is not None:
            storage.close()

    return file_count, row_count


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Replay raw Keepa product JSON files into normalized product snapshot CSV files.",
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Raw Keepa product JSON file or directory.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory to write normalized CSV files into.",
    )
    parser.add_argument(
        "--domain",
        type=int,
        required=True,
        help="Keepa/Amazon domain id used to label marketplace in output rows.",
    )
    parser.add_argument(
        "--duckdb-path",
        help="Optional DuckDB path. When provided, normalized snapshot rows are also ingested into curated.keepa_product_snapshot.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    files, rows = backfill_keepa_product_snapshots(
        input_path=Path(args.input).resolve(),
        output_dir=Path(args.output_dir).resolve(),
        domain=args.domain,
        duckdb_path=Path(args.duckdb_path).resolve() if args.duckdb_path else None,
    )
    print(f"Backfilled {rows} rows from {files} raw JSON file(s).")


if __name__ == "__main__":
    main()
