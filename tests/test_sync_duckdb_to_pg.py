from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from data_collector.sync_duckdb_to_pg import (
    _defer_theme_sync_after_expansion_reconcile,
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


if __name__ == "__main__":
    unittest.main()
