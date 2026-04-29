"""PostgreSQL-backed candidate expansion job queue for the collector."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .pg_runtime import pg_connection_config, pg_connection_configured

try:
    import psycopg2
    import psycopg2.extras
except ImportError:  # pragma: no cover - optional runtime dependency
    psycopg2 = None  # type: ignore[assignment]


@dataclass(frozen=True)
class ExpansionJob:
    job_id: str
    domain: int
    marketplace: str | None
    priority: str
    product_query: str | None
    recall_mode: str | None
    category_id: int | None
    category_path: str | None
    include_descendants: bool
    target_asin_count: int
    tokens_estimated: int
    result_candidate_asins: list[str]
    result_new_asin_count: int

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "ExpansionJob":
        return cls(
            job_id=str(row["job_id"]),
            domain=int(row.get("domain") or 1),
            marketplace=row.get("marketplace"),
            priority=str(row.get("priority") or "interactive_normal"),
            product_query=row.get("product_query"),
            recall_mode=row.get("recall_mode"),
            category_id=int(row["category_id"]) if row.get("category_id") is not None else None,
            category_path=row.get("category_path"),
            include_descendants=bool(row.get("include_descendants")),
            target_asin_count=int(row.get("target_asin_count") or 20),
            tokens_estimated=int(row.get("tokens_estimated") or 0),
            result_candidate_asins=list(row.get("result_candidate_asins") or []),
            result_new_asin_count=int(row.get("result_new_asin_count") or 0),
        )


class ExpansionJobStore:
    def __init__(self) -> None:
        self.enabled = bool(psycopg2 and pg_connection_configured())

    def _connect(self):
        if not self.enabled or psycopg2 is None:
            raise RuntimeError("PostgreSQL expansion job queue is not configured")
        return psycopg2.connect(**pg_connection_config())

    def claim_next_interactive_job(self, *, domain: int) -> ExpansionJob | None:
        if not self.enabled:
            return None
        conn = self._connect()
        try:
            with conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute(
                        """
                        WITH next_job AS (
                            SELECT job_id
                            FROM sync.keepa_candidate_expansion_jobs
                            WHERE domain = %s
                              AND priority IN ('interactive_high', 'interactive_normal')
                              AND status IN ('queued', 'waiting_token')
                            ORDER BY
                              CASE priority
                                WHEN 'interactive_high' THEN 1
                                WHEN 'interactive_normal' THEN 2
                                ELSE 3
                              END,
                              created_at ASC
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE sync.keepa_candidate_expansion_jobs j
                        SET status = 'discovering',
                            status_reason = 'claimed by collector',
                            started_at = COALESCE(started_at, NOW()),
                            updated_at = NOW()
                        FROM next_job
                        WHERE j.job_id = next_job.job_id
                        RETURNING j.*
                        """,
                        [domain],
                    )
                    row = cursor.fetchone()
                    return ExpansionJob.from_row(dict(row)) if row else None
        finally:
            conn.close()

    def claim_next_hydration_job(self, *, domain: int) -> ExpansionJob | None:
        if not self.enabled:
            return None
        conn = self._connect()
        try:
            with conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute(
                        """
                        WITH next_job AS (
                            SELECT job_id
                            FROM sync.keepa_candidate_expansion_jobs
                            WHERE domain = %s
                              AND priority IN ('interactive_high', 'interactive_normal')
                              AND status = 'hydrating'
                            ORDER BY
                              CASE priority
                                WHEN 'interactive_high' THEN 1
                                WHEN 'interactive_normal' THEN 2
                                ELSE 3
                              END,
                              updated_at ASC NULLS LAST,
                              created_at ASC
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE sync.keepa_candidate_expansion_jobs j
                        SET status_reason = 'claimed by collector for hydrate',
                            updated_at = NOW()
                        FROM next_job
                        WHERE j.job_id = next_job.job_id
                        RETURNING j.*
                        """,
                        [domain],
                    )
                    row = cursor.fetchone()
                    return ExpansionJob.from_row(dict(row)) if row else None
        finally:
            conn.close()

    def mark_waiting_token(self, *, job_id: str, tokens_left: int, reason: str) -> None:
        self.update_status(
            job_id=job_id,
            status="waiting_token",
            status_reason=reason,
            tokens_reserved=0,
            tokens_consumed=0,
            error_message=None,
            result_candidate_asins=None,
            result_new_asin_count=None,
            tokens_left=tokens_left,
            tokens_before=tokens_left,
            ledger_action="waiting_token",
            ledger_message=reason,
        )

    def mark_hydrating(
        self,
        *,
        job_id: str,
        result_candidate_asins: list[str],
        result_new_asin_count: int,
        tokens_before: int,
        tokens_after: int,
    ) -> None:
        self.update_status(
            job_id=job_id,
            status="hydrating",
            status_reason="ASINs discovered; collector will hydrate product history next",
            tokens_reserved=0,
            tokens_consumed=max(0, tokens_before - tokens_after),
            error_message=None,
            result_candidate_asins=result_candidate_asins,
            result_new_asin_count=result_new_asin_count,
            tokens_left=tokens_after,
            tokens_before=tokens_before,
            ledger_action="consume",
            ledger_message="interactive expansion discovery consumed Keepa tokens",
        )

    def mark_hydrating_waiting_token(self, *, job_id: str, tokens_left: int, reason: str) -> None:
        self.update_status(
            job_id=job_id,
            status="hydrating",
            status_reason=f"waiting for hydrate tokens: {reason}",
            tokens_reserved=0,
            tokens_consumed=0,
            error_message=None,
            result_candidate_asins=None,
            result_new_asin_count=None,
            tokens_left=tokens_left,
            tokens_before=tokens_left,
            ledger_action="waiting_token",
            ledger_message=reason,
        )

    def mark_syncing(
        self,
        *,
        job_id: str,
        result_candidate_asins: list[str],
        result_new_asin_count: int,
        tokens_before: int,
        tokens_after: int,
    ) -> None:
        self.update_status(
            job_id=job_id,
            status="syncing",
            status_reason="ASINs registered in DuckDB; waiting for DuckDB-to-PostgreSQL sync",
            tokens_reserved=0,
            tokens_consumed=max(0, tokens_before - tokens_after),
            error_message=None,
            result_candidate_asins=result_candidate_asins,
            result_new_asin_count=result_new_asin_count,
            tokens_left=tokens_after,
            tokens_before=tokens_before,
            ledger_action="consume",
            ledger_message="interactive expansion hydrate consumed Keepa tokens",
        )

    def mark_failed(self, *, job_id: str, error_message: str) -> None:
        self.update_status(
            job_id=job_id,
            status="failed",
            status_reason="collector execution failed",
            tokens_reserved=0,
            tokens_consumed=0,
            error_message=error_message,
            result_candidate_asins=None,
            result_new_asin_count=None,
            tokens_left=None,
            tokens_before=None,
            ledger_action="failed",
            ledger_message=error_message,
        )

    def update_status(
        self,
        *,
        job_id: str,
        status: str,
        status_reason: str,
        tokens_reserved: int,
        tokens_consumed: int,
        error_message: str | None,
        result_candidate_asins: list[str] | None,
        result_new_asin_count: int | None,
        tokens_left: int | None,
        tokens_before: int | None = None,
        ledger_action: str | None = None,
        ledger_message: str | None = None,
    ) -> None:
        if not self.enabled:
            return
        conn = self._connect()
        try:
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE sync.keepa_candidate_expansion_jobs
                        SET status = %s,
                            status_reason = %s,
                            tokens_reserved = %s,
                            tokens_consumed = COALESCE(tokens_consumed, 0) + %s,
                            result_candidate_asins = COALESCE(%s::TEXT[], result_candidate_asins),
                            result_new_asin_count = COALESCE(%s, result_new_asin_count),
                            error_message = %s,
                            updated_at = NOW(),
                            finished_at = CASE WHEN %s IN ('completed', 'failed', 'cancelled') THEN NOW() ELSE finished_at END,
                            meta_json = COALESCE(meta_json, '{}'::JSONB) || jsonb_build_object('last_tokens_left', %s)
                        WHERE job_id = %s
                        RETURNING domain, source, priority
                        """,
                        [
                            status,
                            status_reason,
                            tokens_reserved,
                            tokens_consumed,
                            result_candidate_asins,
                            result_new_asin_count,
                            error_message,
                            status,
                            tokens_left,
                            job_id,
                        ],
                    )
                    row = cursor.fetchone()
                    if row and ledger_action:
                        domain = int(row[0] or 1)
                        source = str(row[1] or "collector")
                        priority = str(row[2] or "")
                        tokens_delta = -abs(tokens_consumed) if ledger_action == "consume" else 0
                        cursor.execute(
                            """
                            INSERT INTO sync.keepa_token_ledger (
                                job_id,
                                domain,
                                source,
                                queue_name,
                                action,
                                tokens_before,
                                tokens_delta,
                                tokens_after,
                                status,
                                message,
                                meta_json
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'recorded', %s, %s::JSONB)
                            """,
                            [
                                job_id,
                                domain,
                                source,
                                "interactive" if priority.startswith("interactive") else "background",
                                ledger_action,
                                tokens_before,
                                tokens_delta,
                                tokens_left,
                                ledger_message,
                                psycopg2.extras.Json({"job_status": status, "priority": priority}),
                            ],
                        )
        finally:
            conn.close()
