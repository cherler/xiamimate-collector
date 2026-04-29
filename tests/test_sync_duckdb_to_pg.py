from __future__ import annotations

import unittest

from data_collector.sync_duckdb_to_pg import _reconcile_candidate_expansion_jobs_completion


class FakeCursor:
    def __init__(self, rows: list[tuple[str]]) -> None:
        self.rows = rows
        self.executed_sql = ""

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None

    def execute(self, sql: str) -> None:
        self.executed_sql = sql

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
        self.assertIn("j.status = 'syncing'", conn.cursor_obj.executed_sql)
        self.assertIn("synced_asin_count >= expected_asin_count", conn.cursor_obj.executed_sql)
        self.assertIn("status = 'completed'", conn.cursor_obj.executed_sql)


if __name__ == "__main__":
    unittest.main()