"""Keepa token budgeting helpers for auto-collect and interactive expansion jobs."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any

from .pg_runtime import pg_connection_config, pg_connection_configured

try:
    import psycopg2
    import psycopg2.extras
except ImportError:  # pragma: no cover - optional runtime dependency for PG-aware mode
    psycopg2 = None  # type: ignore[assignment]


@dataclass(frozen=True)
class TokenDecision:
    allowed: bool
    reason: str
    tokens_required: int
    tokens_available_for_queue: int
    tokens_left: int


@dataclass(frozen=True)
class KeepaTokenBudget:
    interactive_min_tokens: int = 150
    bestseller_min_tokens: int = 50
    search_min_tokens: int = 12
    history_min_tokens: int = 2
    safe_reserve_tokens: int = 20
    max_history_tokens_per_run: int = 200
    pause_history_when_interactive_pending: bool = False
    allow_discovery_with_pending: bool = True

    @classmethod
    def from_env(cls) -> "KeepaTokenBudget":
        def _int(name: str, default: int) -> int:
            try:
                return max(0, int(os.environ.get(name, str(default))))
            except ValueError:
                return default

        def _bool(name: str, default: bool) -> bool:
            raw = os.environ.get(name)
            if raw is None:
                return default
            return raw.strip().lower() in {"1", "true", "yes", "on"}

        return cls(
            interactive_min_tokens=_int("KEEPA_INTERACTIVE_MIN_TOKENS", 150),
            bestseller_min_tokens=_int("KEEPA_BESTSELLER_MIN_TOKENS", 50),
            search_min_tokens=_int("KEEPA_SEARCH_MIN_TOKENS", 12),
            history_min_tokens=_int("KEEPA_HISTORY_MIN_TOKENS", 2),
            safe_reserve_tokens=_int("KEEPA_SAFE_RESERVE_TOKENS", 20),
            max_history_tokens_per_run=_int("AUTO_COLLECT_MAX_HISTORY_TOKENS_PER_RUN", 200),
            pause_history_when_interactive_pending=_bool("AUTO_HISTORY_PAUSE_WHEN_INTERACTIVE_PENDING", False),
            allow_discovery_with_pending=_bool("AUTO_DISCOVERY_ALLOW_WHEN_PENDING", True),
        )


class KeepaTokenAllocator:
    """Small policy object that keeps low-token auto-collect from starving discovery jobs."""

    def __init__(self, budget: KeepaTokenBudget | None = None) -> None:
        self.budget = budget or KeepaTokenBudget.from_env()

    @classmethod
    def from_env(cls) -> "KeepaTokenAllocator":
        return cls(KeepaTokenBudget.from_env())

    def can_run(self, *, queue_name: str, tokens_left: int, cost: int, interactive_pending: bool = False) -> TokenDecision:
        reserve = self.budget.safe_reserve_tokens
        if queue_name != "interactive" and interactive_pending:
            reserve = max(reserve, self.budget.interactive_min_tokens)
        available = max(0, int(tokens_left or 0) - reserve)
        allowed = available >= cost
        reason = "allowed" if allowed else f"insufficient_tokens_after_reserve:{available}<{cost}"
        return TokenDecision(
            allowed=allowed,
            reason=reason,
            tokens_required=cost,
            tokens_available_for_queue=available,
            tokens_left=int(tokens_left or 0),
        )

    def history_token_budget(self, *, tokens_left: int, interactive_pending: bool = False) -> TokenDecision:
        if interactive_pending and self.budget.pause_history_when_interactive_pending:
            return TokenDecision(
                allowed=False,
                reason="interactive_expansion_pending_pause_history",
                tokens_required=self.budget.history_min_tokens,
                tokens_available_for_queue=0,
                tokens_left=int(tokens_left or 0),
            )
        decision = self.can_run(
            queue_name="auto_history",
            tokens_left=tokens_left,
            cost=self.budget.history_min_tokens,
            interactive_pending=interactive_pending,
        )
        capped_available = min(decision.tokens_available_for_queue, self.budget.max_history_tokens_per_run)
        return TokenDecision(
            allowed=decision.allowed and capped_available >= self.budget.history_min_tokens,
            reason=decision.reason if decision.allowed else decision.reason,
            tokens_required=self.budget.history_min_tokens,
            tokens_available_for_queue=capped_available,
            tokens_left=decision.tokens_left,
        )

    def has_pending_interactive_jobs(self, *, domain: int | None = None) -> bool:
        if psycopg2 is None:
            return False
        if not pg_connection_configured():
            return False
        try:
            conn = psycopg2.connect(**pg_connection_config())
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    params: list[Any] = []
                    domain_filter = ""
                    if domain is not None:
                        domain_filter = "AND domain = %s"
                        params.append(domain)
                    cursor.execute(
                        f"""
                        SELECT 1
                        FROM sync.keepa_candidate_expansion_jobs
                        WHERE priority IN ('interactive_high', 'interactive_normal')
                          AND status IN ('queued', 'waiting_token', 'discovering', 'hydrating')
                          {domain_filter}
                        LIMIT 1
                        """,
                        params,
                    )
                    return cursor.fetchone() is not None
            finally:
                conn.close()
        except Exception:
            return False
