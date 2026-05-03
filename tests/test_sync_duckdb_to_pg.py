from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from data_collector.sync_duckdb_to_pg import (
    _reconcile_candidate_expansion_jobs_completion,
    _trigger_theme_sync_after_expansion_reconcile,
)


class FakeCursor:
    def __init__(self, rows: list[tuple[str]]) -> None:
        self.rows = rows
        self.executed_sql = ""

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None

    def execute(self, sql: str) -> None:
        self.executed_sql += sql

    def fetchall(self) -> list[tuple[str]]:
        return self.rows


class FakeConnection:
    def __init__(self, rows: list[tuple[str]]) -> None:
        self.cursor_obj = FakeCursor(rows)
        self.commit_count = 0

    def cursor(self) -> FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.commit_count += 1


class SyncExpansionJobsTests(unittest.TestCase):
    def test_reconcile_marks_syncing_jobs_when_all_result_asins_are_in_pg(self) -> None:
        conn = FakeConnection(rows=[("kexp_1",), ("kexp_2",)])

        completed_count = _reconcile_candidate_expansion_jobs_completion(conn)

        self.assertEqual(completed_count, 2)
        self.assertEqual(conn.commit_count, 1)
        self.assertIn("sync.keepa_candidate_expansion_jobs", conn.cursor_obj.executed_sql)
        self.assertIn("sync.keepa_asin_registry", conn.cursor_obj.executed_sql)
        self.assertIn("j.status IN ('syncing', 'completed')", conn.cursor_obj.executed_sql)
        self.assertIn("status = 'hydrating'", conn.cursor_obj.executed_sql)
        self.assertIn("last_fetched_at IS NOT NULL", conn.cursor_obj.executed_sql)
        self.assertIn("j.status = 'syncing'", conn.cursor_obj.executed_sql)
        self.assertIn("synced_asin_count >= expected_asin_count", conn.cursor_obj.executed_sql)
        self.assertIn("fetched_asin_count >= expected_asin_count", conn.cursor_obj.executed_sql)
        self.assertIn("status = 'completed'", conn.cursor_obj.executed_sql)

    def test_theme_sync_trigger_runs_when_expansion_jobs_are_completed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            script_path = Path(tmpdir) / "run_theme_feature_sync_once.sh"
            script_path.write_text("#!/usr/bin/env bash\n", encoding="utf-8")

            with patch("data_collector.sync_duckdb_to_pg.THEME_SYNC_TRIGGER_SCRIPT", script_path), \
                 patch("data_collector.sync_duckdb_to_pg.TRIGGER_THEME_SYNC_ON_EXPANSION_RECONCILE", True), \
                 patch("data_collector.sync_duckdb_to_pg.subprocess.run") as run_mock:
                triggered = _trigger_theme_sync_after_expansion_reconcile(2)

        self.assertTrue(triggered)
        run_mock.assert_called_once()
        self.assertEqual(run_mock.call_args.args[0], ["/bin/bash", str(script_path)])

    def test_theme_sync_trigger_skips_when_no_expansion_jobs_completed(self) -> None:
        with patch("data_collector.sync_duckdb_to_pg.subprocess.run") as run_mock:
            triggered = _trigger_theme_sync_after_expansion_reconcile(0)

        self.assertFalse(triggered)
        run_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()