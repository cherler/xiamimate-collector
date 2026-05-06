#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg2


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def get_pg_conn():
    return psycopg2.connect(
        host=os.environ.get("PG_HOST"),
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ.get("PG_DB"),
        user=os.environ.get("PG_USER"),
        password=os.environ.get("PG_PASSWORD") or os.environ.get("PGPASSWORD"),
    )


def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE SCHEMA IF NOT EXISTS ops;

            CREATE TABLE IF NOT EXISTS ops.forecast_snapshot_runs (
              snapshot_id text PRIMARY KEY,
              source_system text NOT NULL DEFAULT 'ecs2-collector',
              duckdb_path text NOT NULL,
              manifest_path text NOT NULL,
              snapshot_kind text NOT NULL DEFAULT 'duckdb_snapshot',
              feature_manifest_path text,
              snapshot_size_bytes bigint,
              source_db_mtime timestamptz,
              status text NOT NULL,
              started_at timestamptz NOT NULL DEFAULT now(),
              finished_at timestamptz,
              error_message text,
              created_by text NOT NULL DEFAULT 'snapshot-publisher',
              updated_at timestamptz NOT NULL DEFAULT now()
            );

            CREATE TABLE IF NOT EXISTS ops.forecast_training_runs (
              run_id bigserial PRIMARY KEY,
              snapshot_id text NOT NULL REFERENCES ops.forecast_snapshot_runs(snapshot_id),
              worker_id text NOT NULL DEFAULT 'unassigned',
              status text NOT NULL,
              local_snapshot_path text,
              foundation_path text,
              output_root text,
              manifest_path text,
              published_release text,
              started_at timestamptz NOT NULL DEFAULT now(),
              finished_at timestamptz,
              error_stage text,
              error_message text,
              created_at timestamptz NOT NULL DEFAULT now(),
              updated_at timestamptz NOT NULL DEFAULT now()
            );

            CREATE INDEX IF NOT EXISTS idx_forecast_snapshot_runs_status
              ON ops.forecast_snapshot_runs(status, started_at DESC);

            CREATE INDEX IF NOT EXISTS idx_forecast_training_runs_status
              ON ops.forecast_training_runs(status, started_at DESC);

            ALTER TABLE ops.forecast_snapshot_runs
              ADD COLUMN IF NOT EXISTS snapshot_kind text NOT NULL DEFAULT 'duckdb_snapshot';

            ALTER TABLE ops.forecast_snapshot_runs
              ADD COLUMN IF NOT EXISTS feature_manifest_path text;
            """
        )
    conn.commit()


def upsert_snapshot(
    conn,
    *,
    snapshot_id: str,
    duckdb_path: str,
    manifest_path: str,
    snapshot_kind: str,
    feature_manifest_path: str | None,
    snapshot_size_bytes: int | None,
    source_db_mtime: datetime | None,
    status: str,
    source_system: str,
    created_by: str,
    error_message: str | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.forecast_snapshot_runs (
              snapshot_id,
              source_system,
              duckdb_path,
              manifest_path,
              snapshot_kind,
              feature_manifest_path,
              snapshot_size_bytes,
              source_db_mtime,
              status,
              error_message,
              created_by,
              finished_at,
              updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CASE WHEN %s = 'ready' THEN now() ELSE NULL END, now())
            ON CONFLICT (snapshot_id)
            DO UPDATE SET
              source_system = EXCLUDED.source_system,
              duckdb_path = EXCLUDED.duckdb_path,
              manifest_path = EXCLUDED.manifest_path,
              snapshot_kind = EXCLUDED.snapshot_kind,
              feature_manifest_path = EXCLUDED.feature_manifest_path,
              snapshot_size_bytes = COALESCE(EXCLUDED.snapshot_size_bytes, ops.forecast_snapshot_runs.snapshot_size_bytes),
              source_db_mtime = COALESCE(EXCLUDED.source_db_mtime, ops.forecast_snapshot_runs.source_db_mtime),
              status = EXCLUDED.status,
              error_message = EXCLUDED.error_message,
              finished_at = CASE WHEN EXCLUDED.status = 'ready' THEN now() ELSE ops.forecast_snapshot_runs.finished_at END,
              updated_at = now()
            """,
            (
                snapshot_id,
                source_system,
                duckdb_path,
                manifest_path,
                snapshot_kind,
                feature_manifest_path,
                snapshot_size_bytes,
                source_db_mtime,
                status,
                error_message,
                created_by,
                status,
            ),
        )
    conn.commit()


def ensure_training_run(conn, *, snapshot_id: str) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id
            FROM ops.forecast_training_runs
            WHERE snapshot_id = %s
              AND status IN ('pending', 'syncing', 'snapshot_ready', 'foundation_running', 'foundation_ready', 'training', 'publishing', 'published')
            ORDER BY run_id
            LIMIT 1
            """,
            [snapshot_id],
        )
        row = cur.fetchone()
        if row:
            return int(row[0])

        cur.execute(
            """
            INSERT INTO ops.forecast_training_runs (snapshot_id, worker_id, status)
            VALUES (%s, 'unassigned', 'pending')
            RETURNING run_id
            """,
            [snapshot_id],
        )
        run_id = int(cur.fetchone()[0])
    conn.commit()
    return run_id


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report an ECS2 forecast DuckDB snapshot to RDS.")
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--duckdb-path", required=True)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--snapshot-kind", default=os.environ.get("FORECAST_SNAPSHOT_KIND", "duckdb_snapshot"))
    parser.add_argument("--feature-manifest-path", default=os.environ.get("FORECAST_FEATURE_MANIFEST_PATH"))
    parser.add_argument("--snapshot-size-bytes", type=int, default=None)
    parser.add_argument("--source-db-mtime-utc", default=None)
    parser.add_argument("--status", default="ready", choices=["running", "ready", "failed", "expired"])
    parser.add_argument("--source-system", default=os.environ.get("FORECAST_SNAPSHOT_SOURCE_SYSTEM", "ecs2-collector"))
    parser.add_argument("--created-by", default=os.environ.get("FORECAST_SNAPSHOT_CREATED_BY", "forecast-snapshot-publisher"))
    parser.add_argument("--error-message", default=None)
    parser.add_argument("--enqueue-training-run", action="store_true", default=_truthy(os.environ.get("FORECAST_SNAPSHOT_ENQUEUE_TRAINING_RUN", "true")))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_db_mtime = None
    if args.source_db_mtime_utc:
        source_db_mtime = datetime.fromisoformat(args.source_db_mtime_utc.replace("Z", "+00:00"))

    conn = get_pg_conn()
    try:
        ensure_tables(conn)
        upsert_snapshot(
            conn,
            snapshot_id=args.snapshot_id,
            duckdb_path=args.duckdb_path,
            manifest_path=args.manifest_path,
            snapshot_kind=args.snapshot_kind,
            feature_manifest_path=args.feature_manifest_path,
            snapshot_size_bytes=args.snapshot_size_bytes,
            source_db_mtime=source_db_mtime,
            status=args.status,
            source_system=args.source_system,
            created_by=args.created_by,
            error_message=args.error_message,
        )
        run_id = None
        if args.status == "ready" and args.enqueue_training_run:
            run_id = ensure_training_run(conn, snapshot_id=args.snapshot_id)
    finally:
        conn.close()

    print(
        json.dumps(
            {
                "snapshot_id": args.snapshot_id,
                "status": args.status,
                "duckdb_path": args.duckdb_path,
                "manifest_path": args.manifest_path,
                "snapshot_kind": args.snapshot_kind,
                "feature_manifest_path": args.feature_manifest_path,
                "training_run_id": run_id,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())