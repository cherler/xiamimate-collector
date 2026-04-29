from __future__ import annotations

import unittest
from unittest.mock import patch

from data_collector.cross_border_data.expansion_jobs import ExpansionJob
from data_collector.cross_border_data.pg_runtime import pg_connection_config
from data_collector.cross_border_data.token_allocator import KeepaTokenAllocator, KeepaTokenBudget


class FakeCursor:
    def __init__(self) -> None:
        self.statements: list[tuple[str, list[object] | None]] = []
        self.fetchone_count = 0

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None

    def execute(self, sql: str, params: list[object] | None = None) -> None:
        self.statements.append((sql, params))

    def fetchone(self) -> tuple[int, str, str] | None:
        self.fetchone_count += 1
        return (1, "agent_interactive", "interactive_normal")


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_obj = FakeCursor()
        self.closed = False

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None

    def cursor(self) -> FakeCursor:
        return self.cursor_obj

    def close(self) -> None:
        self.closed = True


class KeepaTokenAllocatorTests(unittest.TestCase):
    def test_pg_connection_config_prefers_tunnel_when_enabled(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "PG_TUNNEL_ENABLED": "1",
                "PG_HOST": "remote.pg.internal",
                "PG_PORT": "5432",
                "PG_DB": "xiamimate",
                "PG_USER": "xiamimate",
                "PG_PASSWORD": "secret",
                "PG_TUNNEL_LOCAL_HOST": "127.0.0.1",
                "PG_TUNNEL_LOCAL_PORT": "15432",
            },
            clear=False,
        ):
            config = pg_connection_config()

        self.assertEqual(config["host"], "127.0.0.1")
        self.assertEqual(config["port"], 15432)
        self.assertEqual(config["dbname"], "xiamimate")

    def test_expansion_job_from_row_normalizes_types(self) -> None:
        job = ExpansionJob.from_row(
            {
                "job_id": "kexp_1",
                "domain": "1",
                "marketplace": "US",
                "priority": "interactive_high",
                "product_query": "humidifier",
                "recall_mode": "hybrid",
                "category_id": "12345",
                "category_path": "Home & Kitchen > Humidifiers",
                "include_descendants": True,
                "target_asin_count": "20",
                "tokens_estimated": "90",
            }
        )

        self.assertEqual(job.job_id, "kexp_1")
        self.assertEqual(job.domain, 1)
        self.assertEqual(job.category_id, 12345)
        self.assertEqual(job.target_asin_count, 20)

    def test_expansion_job_status_writes_waiting_token_ledger_event(self) -> None:
        from data_collector.cross_border_data.expansion_jobs import ExpansionJobStore

        store = ExpansionJobStore()
        store.enabled = True
        fake_conn = FakeConnection()
        store._connect = lambda: fake_conn  # type: ignore[method-assign]

        store.mark_waiting_token(job_id="kexp_1", tokens_left=12, reason="insufficient_tokens")

        executed_sql = "\n".join(statement for statement, _params in fake_conn.cursor_obj.statements)
        self.assertIn("UPDATE sync.keepa_candidate_expansion_jobs", executed_sql)
        self.assertIn("INSERT INTO sync.keepa_token_ledger", executed_sql)
        ledger_params = fake_conn.cursor_obj.statements[-1][1]
        self.assertEqual(ledger_params[0], "kexp_1")
        self.assertEqual(ledger_params[4], "waiting_token")
        self.assertEqual(ledger_params[5], 12)
        self.assertEqual(ledger_params[7], 12)
        self.assertTrue(fake_conn.closed)

    def test_auto_discovery_keeps_interactive_reserve(self) -> None:
        allocator = KeepaTokenAllocator(
            KeepaTokenBudget(
                interactive_min_tokens=90,
                bestseller_min_tokens=50,
                safe_reserve_tokens=20,
            )
        )

        decision = allocator.can_run(
            queue_name="auto_discovery",
            tokens_left=120,
            cost=50,
            interactive_pending=True,
        )

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.tokens_available_for_queue, 30)
        self.assertIn("insufficient_tokens_after_reserve", decision.reason)

    def test_auto_discovery_runs_when_above_reserve(self) -> None:
        allocator = KeepaTokenAllocator(
            KeepaTokenBudget(
                interactive_min_tokens=90,
                bestseller_min_tokens=50,
                safe_reserve_tokens=20,
            )
        )

        decision = allocator.can_run(
            queue_name="auto_discovery",
            tokens_left=160,
            cost=50,
            interactive_pending=True,
        )

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.tokens_available_for_queue, 70)

    def test_history_pauses_when_interactive_job_pending(self) -> None:
        allocator = KeepaTokenAllocator(
            KeepaTokenBudget(pause_history_when_interactive_pending=True)
        )

        decision = allocator.history_token_budget(tokens_left=500, interactive_pending=True)

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "interactive_expansion_pending_pause_history")

    def test_history_budget_is_capped_per_run(self) -> None:
        allocator = KeepaTokenAllocator(
            KeepaTokenBudget(
                safe_reserve_tokens=20,
                max_history_tokens_per_run=40,
                pause_history_when_interactive_pending=False,
            )
        )

        decision = allocator.history_token_budget(tokens_left=500, interactive_pending=False)

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.tokens_available_for_queue, 40)


if __name__ == "__main__":
    unittest.main()
