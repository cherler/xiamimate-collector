from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import duckdb

from data_collector.sync_duckdb_to_pg import (
    _AGG_SUBSET_CACHE,
    _AGG_SUBSET_CONNS,
    _SYNC_COPY_DIRS,
    _build_incremental_agg_partitions,
    _build_partitioned_agg_select,
    _create_incremental_agg_subset_duckdb,
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
    def tearDown(self) -> None:
        _AGG_SUBSET_CACHE.clear()
        while _AGG_SUBSET_CONNS:
            try:
                _AGG_SUBSET_CONNS.pop().close()
            except Exception:
                pass
        while _SYNC_COPY_DIRS:
            shutil.rmtree(_SYNC_COPY_DIRS.pop(), ignore_errors=True)

    def test_find_affected_agg_days_uses_ingested_at_and_incremental_date_window(self) -> None:
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
        outside_incremental_window_day = today - timedelta(days=150)
        old_day = today - timedelta(days=1200)
        now_ts = datetime.now()
        stale_ts = now_ts - timedelta(days=30)
        conn.executemany(
            "INSERT INTO curated.keepa_product_history VALUES (?, ?, ?)",
            [
                (1, today, now_ts),
                (1, yesterday, stale_ts),
                (1, outside_incremental_window_day, now_ts),
                (2, old_day, now_ts),
            ],
        )

        affected_days = _find_affected_agg_days(
            conn,
            cutoff_ts=(now_ts - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
        )

        self.assertEqual(affected_days, [(1, today)])

    def test_find_affected_agg_days_raises_when_bucket_limit_exceeded(self) -> None:
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
        now_ts = datetime.now()
        conn.executemany(
            "INSERT INTO curated.keepa_product_history VALUES (?, ?, ?)",
            [
                (1, today, now_ts),
                (1, today - timedelta(days=1), now_ts),
                (1, today - timedelta(days=2), now_ts),
            ],
        )

        with self.assertRaisesRegex(RuntimeError, "PG_SYNC_AGG_MAX_AFFECTED_BUCKETS"):
            _find_affected_agg_days(
                conn,
                cutoff_ts=(now_ts - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
                max_buckets=2,
            )

    def test_create_incremental_agg_subset_keeps_complete_recent_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source_path = Path(tmpdir) / "source.duckdb"
            source = duckdb.connect(str(source_path))
            source.execute("CREATE SCHEMA curated")
            source.execute(
                """
                CREATE TABLE curated.keepa_product_history (
                    asin TEXT,
                    domain INTEGER,
                    date DATE,
                    ingested_at TIMESTAMP,
                    buy_box_price DOUBLE,
                    amazon_price DOUBLE,
                    new_price DOUBLE,
                    bsr INTEGER,
                    rating DOUBLE,
                    review_count INTEGER,
                    monthly_sold INTEGER
                )
                """
            )
            source.execute(
                """
                CREATE TABLE curated.keepa_asin_registry (
                    asin TEXT,
                    domain INTEGER,
                    root_category_id INTEGER,
                    category_id INTEGER,
                    category TEXT
                )
                """
            )
            source.execute(
                """
                CREATE TABLE curated.keepa_category_registry (
                    category_id INTEGER,
                    domain INTEGER,
                    category_cn TEXT,
                    category_en TEXT
                )
                """
            )
            today = date.today()
            now_ts = datetime.now()
            stale_ts = now_ts - timedelta(days=30)
            old_recent_day = today - timedelta(days=44)
            outside_recent_window_day = today - timedelta(days=60)
            source.executemany(
                "INSERT INTO curated.keepa_product_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    ("changed", 1, today, now_ts, 10, None, None, 100, 4.5, 10, 5),
                    ("same_bucket", 1, today, stale_ts, 20, None, None, 200, 4.0, 20, 6),
                    ("other_day", 1, today - timedelta(days=1), stale_ts, 30, None, None, 300, 3.5, 30, 7),
                    ("stale_domain", 2, old_recent_day, stale_ts, 40, None, None, 400, 3.0, 40, 8),
                    ("old_changed", 1, outside_recent_window_day, now_ts, 50, None, None, 500, 2.5, 50, 9),
                ],
            )
            source.executemany(
                "INSERT INTO curated.keepa_asin_registry VALUES (?, ?, ?, ?, ?)",
                [
                    ("changed", 1, 10, 10, "Root"),
                    ("same_bucket", 1, 10, 10, "Root"),
                    ("other_day", 1, 10, 10, "Root"),
                    ("stale_domain", 2, 20, 20, "Root 2"),
                    ("old_changed", 1, 10, 10, "Root"),
                ],
            )
            source.execute("INSERT INTO curated.keepa_category_registry VALUES (10, 1, '根类目', 'Root')")
            source.execute("INSERT INTO curated.keepa_category_registry VALUES (20, 2, '根类目 2', 'Root 2')")
            source.close()

            subset, affected_domains, affected_bucket_count = _create_incremental_agg_subset_duckdb(
                source_path,
                cutoff_ts=(now_ts - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
            )
            self.assertIsNotNone(subset)
            assert subset is not None

            self.assertEqual(affected_domains, [1])
            self.assertEqual(affected_bucket_count, 1)
            self.assertEqual(
                subset.execute("SELECT COUNT(*) FROM curated.keepa_product_history").fetchone()[0],
                4,
            )
            self.assertEqual(
                subset.execute("SELECT COUNT(*) FROM curated.keepa_asin_registry").fetchone()[0],
                4,
            )
            self.assertEqual(
                subset.execute("SELECT COUNT(*) FROM curated.keepa_category_registry").fetchone()[0],
                2,
            )
            self.assertEqual(
                subset.execute("SELECT COUNT(*) FROM curated.keepa_product_history WHERE asin = 'old_changed'").fetchone()[0],
                0,
            )
            subset.close()

    def test_incremental_agg_partitions_use_domain_and_us_months(self) -> None:
        conn = duckdb.connect(":memory:")
        self.addCleanup(conn.close)

        partitions = _build_incremental_agg_partitions(conn, [1, 2], date_window_days=45)

        us_partitions = [partition for partition in partitions if partition[0] == 1]
        other_domain_partitions = [partition for partition in partitions if partition[0] == 2]
        self.assertGreaterEqual(len(us_partitions), 2)
        self.assertEqual(len(other_domain_partitions), 1)
        self.assertIn("recent_45d", other_domain_partitions[0][3])
        self.assertTrue(all("month=" in partition[3] for partition in us_partitions))

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
