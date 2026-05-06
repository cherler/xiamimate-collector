#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import psycopg2


def parse_csv(raw_value: str) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for item in raw_value.split(","):
        value = item.strip()
        if not value or value in seen:
            continue
        values.append(value)
        seen.add(value)
    return values


def get_pg_job_rows(job_ids: list[str]) -> list[tuple[int, str]]:
    conn = psycopg2.connect(
        host=os.environ.get("PG_HOST"),
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ.get("PG_DB"),
        user=os.environ.get("PG_USER"),
        password=os.environ.get("PG_PASSWORD"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT j.domain, asin
                FROM sync.keepa_candidate_expansion_jobs j
                CROSS JOIN LATERAL unnest(COALESCE(j.result_candidate_asins, ARRAY[]::TEXT[])) AS asin
                WHERE j.job_id = ANY(%s)
                ORDER BY j.domain, asin
                """,
                [job_ids],
            )
            return [(int(domain), str(asin)) for domain, asin in cur.fetchall()]
    finally:
        conn.close()


def quote_sql(value: str) -> str:
    return value.replace("'", "''")


def source_table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    schema_name, simple_name = table_name.split(".", 1)
    return bool(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_catalog = 'src'
              AND table_schema = ?
              AND table_name = ?
            """,
            [schema_name, simple_name],
        ).fetchone()[0]
    )


def create_empty_like(conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM src.{table_name} WHERE FALSE")


def create_target_filtered(conn: duckdb.DuckDBPyConnection, table_name: str) -> int:
    conn.execute(
        f"""
        CREATE TABLE {table_name} AS
        SELECT s.*
        FROM src.{table_name} s
        WHERE EXISTS (
            SELECT 1
            FROM target_asins t
            WHERE t.asin = s.asin AND t.domain = s.domain
        )
        """
    )
    return int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def build_subset(
    *,
    source_db: Path,
    output_db: Path,
    job_rows: list[tuple[int, str]],
    job_ids: list[str],
) -> dict[str, object]:
    output_db.parent.mkdir(parents=True, exist_ok=True)
    if output_db.exists():
        output_db.unlink()
    wal_path = Path(f"{output_db}.wal")
    if wal_path.exists():
        wal_path.unlink()

    conn = duckdb.connect(str(output_db))
    try:
        conn.execute(f"ATTACH '{quote_sql(str(source_db))}' AS src (READ_ONLY)")
        conn.execute("CREATE SCHEMA IF NOT EXISTS curated")
        conn.execute("CREATE TEMP TABLE target_asins(domain INTEGER, asin TEXT)")
        conn.executemany("INSERT INTO target_asins VALUES (?, ?)", job_rows)

        row_counts: dict[str, int] = {}
        target_tables = [
            "curated.keepa_asin_registry",
            "curated.keepa_product_snapshot",
            "curated.keepa_product_history",
            "curated.asin_keyword_mapping",
            "curated.asin_raw_file_mapping",
        ]
        for table_name in target_tables:
            if source_table_exists(conn, table_name):
                row_counts[table_name] = create_target_filtered(conn, table_name)

        if source_table_exists(conn, "curated.discovery_expansion_state"):
            conn.execute(
                """
                CREATE TABLE curated.discovery_expansion_state AS
                SELECT * FROM src.curated.discovery_expansion_state
                """
            )
            row_counts["curated.discovery_expansion_state"] = int(
                conn.execute("SELECT COUNT(*) FROM curated.discovery_expansion_state").fetchone()[0]
            )

        if source_table_exists(conn, "curated.google_trends_daily") and source_table_exists(conn, "curated.asin_keyword_mapping"):
            conn.execute(
                """
                CREATE TABLE curated.google_trends_daily AS
                SELECT DISTINCT g.*
                FROM src.curated.google_trends_daily g
                JOIN src.curated.asin_keyword_mapping m
                  ON g.keyword = m.keyword AND g.geo = m.geo
                JOIN target_asins t
                  ON t.asin = m.asin AND t.domain = m.domain
                """
            )
            row_counts["curated.google_trends_daily"] = int(
                conn.execute("SELECT COUNT(*) FROM curated.google_trends_daily").fetchone()[0]
            )

        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    domains = sorted({domain for domain, _ in job_rows})
    asins = sorted({asin for _, asin in job_rows})
    manifest = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_db": str(source_db),
        "subset_db": str(output_db),
        "job_ids": job_ids,
        "asin_count": len(asins),
        "domains": domains,
        "row_counts": row_counts,
    }
    output_db.with_suffix(".manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return manifest


def emit_shell(manifest: dict[str, object], job_rows: list[tuple[int, str]]) -> None:
    asins = sorted({asin for _, asin in job_rows})
    domains = sorted({domain for domain, _ in job_rows})
    values = {
        "CANDIDATE_EXPANSION_SUBSET_DUCKDB_PATH": str(manifest["subset_db"]),
        "CANDIDATE_EXPANSION_TARGET_ASINS": ",".join(asins),
        "CANDIDATE_EXPANSION_TARGET_DOMAINS": ",".join(str(domain) for domain in domains),
    }
    for key, value in values.items():
        print(f"export {key}={shlex.quote(value)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a small DuckDB subset for candidate expansion refresh.")
    parser.add_argument("--source-db", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--job-ids", default=os.environ.get("CANDIDATE_EXPANSION_JOB_IDS", ""))
    parser.add_argument("--retries", type=int, default=int(os.environ.get("CANDIDATE_EXPANSION_DUCKDB_OPEN_RETRIES", "30")))
    parser.add_argument(
        "--retry-delay-seconds",
        type=float,
        default=float(os.environ.get("CANDIDATE_EXPANSION_DUCKDB_OPEN_RETRY_DELAY_SECONDS", "2")),
    )
    parser.add_argument("--emit-shell", action="store_true")
    args = parser.parse_args()

    job_ids = parse_csv(args.job_ids)
    if not job_ids:
        print("candidate expansion subset: no job ids supplied", file=sys.stderr)
        return 2

    job_rows = get_pg_job_rows(job_ids)
    if not job_rows:
        print("export CANDIDATE_EXPANSION_SUBSET_DUCKDB_PATH=''" if args.emit_shell else "{}")
        return 0

    source_db = Path(args.source_db).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_db = output_dir / f"candidate_expansion_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{os.getpid()}.duckdb"

    last_exc: Exception | None = None
    retries = max(1, args.retries)
    for attempt in range(1, retries + 1):
        try:
            manifest = build_subset(source_db=source_db, output_db=output_db, job_rows=job_rows, job_ids=job_ids)
            if args.emit_shell:
                emit_shell(manifest, job_rows)
            else:
                print(json.dumps(manifest, ensure_ascii=False, indent=2))
            return 0
        except Exception as exc:  # noqa: BLE001 - keep retry broad around DuckDB file locks
            last_exc = exc
            try:
                output_db.unlink(missing_ok=True)
                Path(f"{output_db}.wal").unlink(missing_ok=True)
            except Exception:
                pass
            if attempt >= retries:
                break
            wait_seconds = min(max(args.retry_delay_seconds, 0.2) * (2 ** (attempt - 1)), 12.0)
            print(
                f"candidate expansion subset: DuckDB open/build failed ({attempt}/{retries}), retry in {wait_seconds:.1f}s: {exc}",
                file=sys.stderr,
            )
            time.sleep(wait_seconds)

    print(f"candidate expansion subset failed: {last_exc}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())