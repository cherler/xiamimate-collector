#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


HISTORY_COLUMNS = [
    "asin",
    "domain",
    "date",
    "amazon_price",
    "new_price",
    "used_price",
    "buy_box_price",
    "list_price",
    "bsr",
    "rating",
    "review_count",
    "monthly_sold",
    "new_offer_count",
    "used_offer_count",
    "ingested_at",
]

REGISTRY_COLUMNS = [
    "asin",
    "domain",
    "marketplace",
    "product_title",
    "brand",
    "category",
    "category_id",
    "category_path",
    "root_category_id",
    "discovery_source",
    "search_term",
    "priority",
    "business_score_total",
    "business_tier",
    "business_priority",
    "score_updated_at",
    "first_seen_at",
    "last_fetched_at",
    "last_snapshot_at",
    "fetch_count",
    "is_active",
    "inactive_reason",
    "inactive_at",
    "notes",
]


def _truthy(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_domains(raw: str | None) -> list[int] | None:
    if not raw:
        return None
    domains: list[int] = []
    for token in raw.replace(",", " ").split():
        token = token.strip()
        if token:
            domains.append(int(token))
    return sorted(set(domains)) or None


def _sql_string(value: str | Path) -> str:
    return str(value).replace("'", "''")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _entry_key(entry: dict[str, Any]) -> str:
    return "|".join(
        [
            str(entry["table"]),
            str(entry.get("domain", "")),
            str(entry.get("partition_key", "")),
        ]
    )


def _copy_or_link(source_path: Path, target_path: Path) -> bool:
    if not source_path.exists():
        return False
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists():
        target_path.unlink()
    try:
        os.link(source_path, target_path)
    except OSError:
        shutil.copy2(source_path, target_path)
    return True


def _replace_current_link(current_link: Path, target_dir: Path) -> None:
    current_link.parent.mkdir(parents=True, exist_ok=True)
    tmp_link = current_link.with_name(f".{current_link.name}.tmp")
    if tmp_link.exists() or tmp_link.is_symlink():
        tmp_link.unlink()
    os.symlink(target_dir, tmp_link)
    os.replace(tmp_link, current_link)


def _cleanup_old_runs(runs_dir: Path, keep_dir: Path, retention_count: int) -> None:
    if retention_count < 1:
        return
    runs = sorted(
        [path for path in runs_dir.iterdir() if path.is_dir() and path != keep_dir],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for old_run in runs[max(0, retention_count - 1):]:
        shutil.rmtree(old_run, ignore_errors=True)


def _domain_filter_sql(domains: list[int] | None, column_name: str = "domain") -> str:
    if not domains:
        return ""
    values = ", ".join(str(domain) for domain in domains)
    return f" AND {column_name} IN ({values})"


def _history_checksum_expr() -> str:
    columns = ", ".join(HISTORY_COLUMNS)
    return f"SUM(CAST(hash({columns}) AS HUGEINT))"


def _registry_checksum_expr() -> str:
    columns = ", ".join(REGISTRY_COLUMNS)
    return f"SUM(CAST(hash({columns}) AS HUGEINT))"


def _discover_domains(conn: duckdb.DuckDBPyConnection, requested_domains: list[int] | None) -> list[int]:
    if requested_domains:
        return requested_domains
    rows = conn.execute(
        """
        SELECT DISTINCT domain
        FROM curated.keepa_asin_registry
        WHERE domain IS NOT NULL
        ORDER BY domain
        """
    ).fetchall()
    return [int(row[0]) for row in rows]


def _history_partition_stats(
    conn: duckdb.DuckDBPyConnection,
    *,
    domains: list[int],
    history_days: int,
) -> list[dict[str, Any]]:
    domain_filter = _domain_filter_sql(domains, "domain")
    rows = conn.execute(
        f"""
        SELECT
            domain,
            CAST(date_trunc('week', date) AS DATE) AS week_start,
            COUNT(*) AS row_count,
            MIN(date) AS min_date,
            MAX(date) AS max_date,
            MIN(ingested_at) AS min_ingested_at,
            MAX(ingested_at) AS max_ingested_at,
            {_history_checksum_expr()} AS business_checksum
        FROM curated.keepa_product_history
        WHERE date >= CURRENT_DATE - INTERVAL {int(history_days)} DAY
          {domain_filter}
        GROUP BY 1, 2
        ORDER BY 1, 2
        """
    ).fetchall()
    stats: list[dict[str, Any]] = []
    for row in rows:
        domain, week_start, row_count, min_date, max_date, min_ingested_at, max_ingested_at, checksum = row
        partition_key = f"week_start={week_start.isoformat()}"
        relative_path = f"tables/curated.keepa_product_history/domain={int(domain)}/{partition_key}/part-000.parquet"
        stats.append(
            {
                "table": "curated.keepa_product_history",
                "domain": int(domain),
                "partition_key": partition_key,
                "min_date": min_date.isoformat() if min_date else None,
                "max_date": max_date.isoformat() if max_date else None,
                "row_count": int(row_count),
                "min_ingested_at": min_ingested_at.isoformat() if min_ingested_at else None,
                "max_ingested_at": max_ingested_at.isoformat() if max_ingested_at else None,
                "business_checksum": str(checksum or 0),
                "relative_path": relative_path,
            }
        )
    return stats


def _registry_partition_stats(
    conn: duckdb.DuckDBPyConnection,
    *,
    domains: list[int],
) -> list[dict[str, Any]]:
    domain_filter = _domain_filter_sql(domains, "domain")
    rows = conn.execute(
        f"""
        SELECT
            domain,
            COUNT(*) AS row_count,
            MIN(first_seen_at) AS min_first_seen_at,
            MAX(first_seen_at) AS max_first_seen_at,
            MAX(last_fetched_at) AS max_last_fetched_at,
            MAX(score_updated_at) AS max_score_updated_at,
            {_registry_checksum_expr()} AS business_checksum
        FROM curated.keepa_asin_registry
        WHERE 1 = 1
          {domain_filter}
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchall()
    stats: list[dict[str, Any]] = []
    for row in rows:
        domain, row_count, min_first_seen_at, max_first_seen_at, max_last_fetched_at, max_score_updated_at, checksum = row
        relative_path = f"tables/curated.keepa_asin_registry/domain={int(domain)}/registry.parquet"
        stats.append(
            {
                "table": "curated.keepa_asin_registry",
                "domain": int(domain),
                "partition_key": "registry",
                "row_count": int(row_count),
                "min_first_seen_at": min_first_seen_at.isoformat() if min_first_seen_at else None,
                "max_first_seen_at": max_first_seen_at.isoformat() if max_first_seen_at else None,
                "max_last_fetched_at": max_last_fetched_at.isoformat() if max_last_fetched_at else None,
                "max_score_updated_at": max_score_updated_at.isoformat() if max_score_updated_at else None,
                "business_checksum": str(checksum or 0),
                "relative_path": relative_path,
            }
        )
    return stats


def _finalize_file_entry(run_dir: Path, entry: dict[str, Any], reused: bool) -> dict[str, Any]:
    file_path = run_dir / entry["relative_path"]
    entry["file_size_bytes"] = file_path.stat().st_size
    entry["file_sha256"] = _sha256_file(file_path)
    entry["reused_from_previous_run"] = reused
    return entry


def _export_history_partition(conn: duckdb.DuckDBPyConnection, *, run_dir: Path, entry: dict[str, Any]) -> None:
    domain = int(entry["domain"])
    week_start = entry["partition_key"].split("=", 1)[1]
    output_path = run_dir / entry["relative_path"]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()
    selected_columns = ", ".join(HISTORY_COLUMNS)
    conn.execute(
        f"""
        COPY (
            SELECT {selected_columns}
            FROM curated.keepa_product_history
            WHERE domain = {domain}
              AND date >= DATE '{week_start}'
              AND date < DATE '{week_start}' + INTERVAL 7 DAY
            ORDER BY asin, domain, date
        ) TO '{_sql_string(output_path)}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """
    )


def _export_registry_partition(conn: duckdb.DuckDBPyConnection, *, run_dir: Path, entry: dict[str, Any]) -> None:
    domain = int(entry["domain"])
    output_path = run_dir / entry["relative_path"]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()
    selected_columns = ", ".join(REGISTRY_COLUMNS)
    conn.execute(
        f"""
        COPY (
            SELECT {selected_columns}
            FROM curated.keepa_asin_registry
            WHERE domain = {domain}
            ORDER BY asin, domain
        ) TO '{_sql_string(output_path)}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """
    )


def _build_manifest(
    *,
    snapshot_id: str,
    source_db: Path,
    run_dir: Path,
    history_days: int,
    domains: list[int],
    entries: list[dict[str, Any]],
) -> dict[str, Any]:
    history_entries = [entry for entry in entries if entry["table"] == "curated.keepa_product_history"]
    high_watermarks = [entry.get("max_ingested_at") for entry in history_entries if entry.get("max_ingested_at")]
    return {
        "manifest_version": 1,
        "snapshot_kind": "feature_parquet",
        "snapshot_id": snapshot_id,
        "created_at_utc": _utc_now(),
        "source_system": "ecs2-collector",
        "source_db": str(source_db),
        "run_dir": str(run_dir),
        "history_days": history_days,
        "domains": domains,
        "high_watermark_ingested_at": max(high_watermarks) if high_watermarks else None,
        "tables": ["curated.keepa_product_history", "curated.keepa_asin_registry"],
        "entries": entries,
    }


def _report_rds(
    *,
    args: argparse.Namespace,
    manifest_path: Path,
    snapshot_id: str,
    total_size: int,
) -> None:
    report_script = Path(args.report_script).resolve()
    command = [
        sys.executable,
        str(report_script),
        "--snapshot-id",
        snapshot_id,
        "--duckdb-path",
        str(manifest_path),
        "--manifest-path",
        str(manifest_path),
        "--snapshot-size-bytes",
        str(total_size),
        "--status",
        "ready",
        "--snapshot-kind",
        "feature_parquet",
        "--feature-manifest-path",
        str(manifest_path),
    ]
    if args.enqueue_training_run:
        command.append("--enqueue-training-run")
    subprocess.run(command, check=True)


def parse_args() -> argparse.Namespace:
    default_base_dir = os.environ.get("FORECAST_FEATURE_EXPORT_BASE_DIR", "/data/xiamimate/forecast_feature_exports")
    default_source_db = os.environ.get(
        "FORECAST_FEATURE_EXPORT_SOURCE_DB",
        os.environ.get("XIAMIMATE_DUCKDB_SNAPSHOT_CURRENT_LINK", "/data/xiamimate/duckdb/snapshots/current") + "/local_analytics.duckdb",
    )
    parser = argparse.ArgumentParser(description="Export forecast feature parquet partitions from ECS2 DuckDB.")
    parser.add_argument("--source-db", default=default_source_db)
    parser.add_argument("--base-dir", default=default_base_dir)
    parser.add_argument("--snapshot-id", default=os.environ.get("FORECAST_SNAPSHOT_ID"))
    parser.add_argument("--domains", default=os.environ.get("FORECAST_FEATURE_EXPORT_DOMAINS") or os.environ.get("DOMAINS"))
    parser.add_argument("--history-days", type=int, default=int(os.environ.get("FORECAST_FEATURE_EXPORT_HISTORY_DAYS", "365")))
    parser.add_argument("--retention-count", type=int, default=int(os.environ.get("FORECAST_FEATURE_EXPORT_RETENTION_COUNT", "1")))
    parser.add_argument("--report-rds", action="store_true", default=_truthy(os.environ.get("FORECAST_FEATURE_EXPORT_REPORT_RDS", "true"), True))
    parser.add_argument("--enqueue-training-run", action="store_true", default=_truthy(os.environ.get("FORECAST_SNAPSHOT_ENQUEUE_TRAINING_RUN", "true"), True))
    parser.add_argument("--report-script", default=os.environ.get("FORECAST_SNAPSHOT_REPORT_SCRIPT", str(Path(__file__).with_name("report_forecast_snapshot_ready.py"))))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_db = Path(args.source_db).resolve()
    if not source_db.exists():
        raise FileNotFoundError(f"source DuckDB not found: {source_db}")

    snapshot_id = args.snapshot_id or f"forecast_features_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    base_dir = Path(args.base_dir).resolve()
    runs_dir = base_dir / "runs"
    current_link = base_dir / "current"
    run_dir = runs_dir / snapshot_id
    manifest_path = run_dir / "export_manifest.json"

    if run_dir.exists():
        raise FileExistsError(f"forecast feature export run already exists: {run_dir}")
    run_dir.mkdir(parents=True)

    previous_manifest = _load_json(current_link / "export_manifest.json" if current_link.exists() else None)
    previous_run_dir = Path(previous_manifest["run_dir"]) if previous_manifest and previous_manifest.get("run_dir") else None
    previous_entries = {_entry_key(entry): entry for entry in (previous_manifest or {}).get("entries", [])}

    conn = duckdb.connect(str(source_db), read_only=True)
    try:
        domains = _discover_domains(conn, _parse_domains(args.domains))
        history_stats = _history_partition_stats(conn, domains=domains, history_days=args.history_days)
        registry_stats = _registry_partition_stats(conn, domains=domains)

        entries: list[dict[str, Any]] = []
        exported_count = 0
        reused_count = 0

        for entry in [*history_stats, *registry_stats]:
            key = _entry_key(entry)
            previous = previous_entries.get(key)
            target_path = run_dir / entry["relative_path"]
            reused = False
            if (
                previous_run_dir is not None
                and previous
                and previous.get("business_checksum") == entry.get("business_checksum")
                and previous.get("row_count") == entry.get("row_count")
            ):
                previous_path = previous_run_dir / previous["relative_path"]
                reused = _copy_or_link(previous_path, target_path)

            if not reused:
                if entry["table"] == "curated.keepa_product_history":
                    _export_history_partition(conn, run_dir=run_dir, entry=entry)
                elif entry["table"] == "curated.keepa_asin_registry":
                    _export_registry_partition(conn, run_dir=run_dir, entry=entry)
                else:
                    raise ValueError(f"unsupported table: {entry['table']}")
                exported_count += 1
            else:
                reused_count += 1

            entries.append(_finalize_file_entry(run_dir, entry, reused))
    finally:
        conn.close()

    manifest = _build_manifest(
        snapshot_id=snapshot_id,
        source_db=source_db,
        run_dir=run_dir,
        history_days=args.history_days,
        domains=domains,
        entries=entries,
    )
    manifest["exported_partition_count"] = exported_count
    manifest["reused_partition_count"] = reused_count
    manifest["total_file_size_bytes"] = sum(int(entry["file_size_bytes"]) for entry in entries)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    _replace_current_link(current_link, run_dir)
    _cleanup_old_runs(runs_dir, run_dir, args.retention_count)

    if args.report_rds:
        _report_rds(args=args, manifest_path=manifest_path, snapshot_id=snapshot_id, total_size=manifest["total_file_size_bytes"])

    print(
        json.dumps(
            {
                "snapshot_id": snapshot_id,
                "run_dir": str(run_dir),
                "manifest_path": str(manifest_path),
                "entries": len(entries),
                "exported_partition_count": exported_count,
                "reused_partition_count": reused_count,
                "total_file_size_bytes": manifest["total_file_size_bytes"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
