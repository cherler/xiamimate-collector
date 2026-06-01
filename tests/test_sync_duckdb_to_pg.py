from __future__ import annotations

import os
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import duckdb

from data_collector.sync_duckdb_to_pg import (
    _build_partitioned_agg_select,
    _defer_theme_sync_after_expansion_reconcile,
    _find_affected_agg_days,
    _reconcile_candidate_expansion_jobs_completion,
)


class FakeCursor:
    def __init__(self, rows_per_execute: list[list[tuple[str]]]) -> None:
        self._rows_per_execute = list(rows_per_execute)
        self._last_rows: list[tuple[str]] = []
        self.executed_sql = ""

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None

    def execute(self, sql: str) -> None:
        self.executed_sql += sql
        self._last_rows = self._rows_per_execute.pop(0) if self._rows_per_execute else []

    def fetchall(self) -> list[tuple[str]]:
        return self._last_rows


class FakeConnection:
    def __init__(self, rows_per_execute: list[list[tuple[str]]]) -> None:
        self.cursor_obj = FakeCursor(rows_per_execute)
        self.commit_count = 0

    def cursor(self) -> FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.commit_count += 1


class SyncExpansionJobsTests(unittest.TestCase):
    def test_reconcile_returns_completed_job_ids(self) -> None:
        conn = FakeConnection(rows_per_execute=[[], [("kexp_1",), ("kexp_2",)]])

        completed_job_ids = _reconcile_candidate_expansion_jobs_completion(conn)

        self.assertEqual(completed_job_ids, ["kexp_1", "kexp_2"])
        self.assertEqual(conn.commit_count, 1)
        self.assertIn("sync.keepa_candidate_expansion_jobs", conn.cursor_obj.executed_sql)
        self.assertIn("sync.keepa_asin_registry", conn.cursor_obj.executed_sql)
        self.assertIn("status = 'hydrating'", conn.cursor_obj.executed_sql)
        self.assertIn("last_fetched_at IS NOT NULL", conn.cursor_obj.executed_sql)
        self.assertIn("status = 'completed'", conn.cursor_obj.executed_sql)
        # Pass 1 must only requeue jobs currently in 'syncing' to avoid reverting
        # already-completed jobs back to hydrating.
        self.assertNotIn("j.status IN ('syncing', 'completed')", conn.cursor_obj.executed_sql)

    def test_defer_writes_completed_job_ids_to_env_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "reconciled_expansion_job_ids.txt"
            with patch.dict(
                os.environ,
                {"PG_SYNC_RECONCILED_EXPANSION_JOB_IDS_FILE": str(output_path)},
                clear=False,
            ):
                deferred = _defer_theme_sync_after_expansion_reconcile(["kexp_1", "kexp_2"])

            self.assertTrue(deferred)
            self.assertEqual(
                output_path.read_text(encoding="utf-8").splitlines(),
                ["kexp_1", "kexp_2"],
            )

    def test_defer_no_op_when_env_file_unset(self) -> None:
        env = {k: v for k, v in os.environ.items() if k != "PG_SYNC_RECONCILED_EXPANSION_JOB_IDS_FILE"}
        with patch.dict(os.environ, env, clear=True):
            self.assertFalse(_defer_theme_sync_after_expansion_reconcile(["kexp_1"]))


class AggIncrementalTests(unittest.TestCase):
    def test_find_affected_agg_days_uses_ingested_at_and_history_window(self) -> None:
        conn = duckdb.connect(":memory:")
        self.addCleanup(conn.close)
        conn.execute("CREATE SCHEMA curated")
        conn.execute(
            """
            CREATE TABLE curated.keepa_product_history (
                domain INTEGER,
                date DATE,
                ingested_at TIMESTAMP
            )
            """
        )
        today = date.today()
        yesterday = today - timedelta(days=1)
        old_day = today - timedelta(days=1200)
        now_ts = datetime.now()
        stale_ts = now_ts - timedelta(days=30)
        conn.executemany(
            "INSERT INTO curated.keepa_product_history VALUES (?, ?, ?)",
            [
                (1, today, now_ts),
                (1, yesterday, stale_ts),
                (2, old_day, now_ts),
            ],
        )

        affected_days = _find_affected_agg_days(
            conn,
            cutoff_ts=(now_ts - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
        )

        self.assertEqual(affected_days, [(1, today)])

    def test_build_partitioned_agg_select_scopes_single_day(self) -> None:
        query = _build_partitioned_agg_select(
            {
                "duck_sql_template": "SELECT h.date, h.domain FROM curated.keepa_product_history h {where_clause}"
            },
            domain=1,
            start_date=date(2026, 5, 1),
            end_date=date(2026, 5, 2),
        )

        self.assertIn("h.domain = 1", query)
        self.assertIn("h.date >= DATE '2026-05-01'", query)
        self.assertIn("h.date < DATE '2026-05-02'", query)


if __name__ == "__main__":
    unittest.main()
