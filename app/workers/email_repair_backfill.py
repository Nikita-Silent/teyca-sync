"""One-time duplicate-email normalization for current DB state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.listmonk import ListmonkClientError, ListmonkSDKClient
from app.clients.teyca import TeycaAPIError, TeycaClient
from app.config import Settings, get_settings
from app.db.session import SessionLocal
from app.repositories.email_repair_log import EmailRepairLogRepository
from app.repositories.listmonk_users import ListmonkUsersRepository
from app.repositories.users import UsersRepository
from app.workers.email_repair_worker import EMAIL_REPAIR_MAX_ATTEMPTS, TEYCA_KEY1_BAD_EMAIL

logger = structlog.get_logger()


@dataclass(slots=True)
class DuplicateEmailBackfillPlan:
    """Resolved duplicate-email cleanup plan for one normalized email."""

    normalized_email: str
    winner_user_id: int
    winner_subscriber_id: int
    loser_user_ids: list[int]


@dataclass(slots=True)
class DuplicateEmailBackfillIssue:
    """Unresolved duplicate-email group that requires manual handling."""

    normalized_email: str
    candidate_user_ids: list[int]
    error: str


@dataclass(slots=True)
class DuplicateEmailBackfillSummary:
    """Aggregated counts for one backfill action."""

    duplicate_emails: int = 0
    resolved_emails: int = 0
    unresolved_emails: int = 0
    loser_rows: int = 0
    teyca_synced: int = 0
    teyca_failed: int = 0
    manual_review: int = 0


@dataclass(slots=True)
class DuplicateEmailBackfill:
    """Backfill helper for current duplicate-email data cleanup."""

    settings: Settings
    session_factory: async_sessionmaker[AsyncSession]
    listmonk_client: ListmonkSDKClient
    teyca_client: TeycaClient

    async def collect_plans(
        self,
        *,
        limit: int | None = None,
    ) -> tuple[list[DuplicateEmailBackfillPlan], list[DuplicateEmailBackfillIssue]]:
        """Resolve current duplicate emails into winner/loser plans."""
        async with self.session_factory() as session:
            listmonk_repo = ListmonkUsersRepository(session)
            duplicate_emails = await listmonk_repo.get_duplicate_emails(limit=limit)

            plans: list[DuplicateEmailBackfillPlan] = []
            issues: list[DuplicateEmailBackfillIssue] = []
            for normalized_email in duplicate_emails:
                rows = await listmonk_repo.get_by_email(email=normalized_email)
                candidate_user_ids = [int(row.user_id) for row in rows]
                try:
                    subscriber = await self.listmonk_client.get_subscriber_by_email(
                        email=normalized_email
                    )
                    if subscriber is None:
                        raise DuplicateEmailBackfillError(
                            "subscriber_by_email returned no subscriber"
                        )
                    winner_row = await listmonk_repo.get_by_subscriber_id(
                        subscriber_id=subscriber.subscriber_id
                    )
                    if winner_row is None:
                        raise DuplicateEmailBackfillError(
                            "subscriber_id="
                            f"{subscriber.subscriber_id} is not mapped in listmonk_users"
                        )
                    winner_user_id = int(winner_row.user_id)
                    if winner_user_id not in candidate_user_ids:
                        raise DuplicateEmailBackfillError(
                            "winner_user_id="
                            f"{winner_user_id} is outside duplicate group {candidate_user_ids}"
                        )
                    loser_user_ids = [
                        user_id for user_id in candidate_user_ids if user_id != winner_user_id
                    ]
                    plans.append(
                        DuplicateEmailBackfillPlan(
                            normalized_email=normalized_email,
                            winner_user_id=winner_user_id,
                            winner_subscriber_id=subscriber.subscriber_id,
                            loser_user_ids=loser_user_ids,
                        )
                    )
                except (DuplicateEmailBackfillError, ListmonkClientError) as exc:
                    issues.append(
                        DuplicateEmailBackfillIssue(
                            normalized_email=normalized_email,
                            candidate_user_ids=candidate_user_ids,
                            error=str(exc),
                        )
                    )

        return plans, issues

    async def apply(
        self,
        *,
        plans: list[DuplicateEmailBackfillPlan],
        issues: list[DuplicateEmailBackfillIssue],
    ) -> DuplicateEmailBackfillSummary:
        """Atomically normalize local DB state for all resolved duplicate emails."""
        if issues:
            raise DuplicateEmailBackfillError(
                "Cannot apply duplicate-email backfill while unresolved groups remain"
            )

        run_id = f"email-backfill:{datetime.now(UTC).isoformat()}"
        summary = DuplicateEmailBackfillSummary(
            duplicate_emails=len(plans),
            resolved_emails=len(plans),
            loser_rows=sum(len(plan.loser_user_ids) for plan in plans),
        )

        async with self.session_factory() as session:
            users_repo = UsersRepository(session)
            listmonk_repo = ListmonkUsersRepository(session)
            repair_repo = EmailRepairLogRepository(session)

            for plan in plans:
                for loser_user_id in plan.loser_user_ids:
                    await users_repo.clear_email(user_id=loser_user_id)
                    await listmonk_repo.clear_email(user_id=loser_user_id)
                    await repair_repo.create_db_applied(
                        normalized_email=plan.normalized_email,
                        incoming_user_id=loser_user_id,
                        existing_user_id=plan.winner_user_id,
                        winner_user_id=plan.winner_user_id,
                        winner_subscriber_id=plan.winner_subscriber_id,
                        source_event_id=run_id,
                        trace_id=run_id,
                    )

            await session.commit()

        logger.info(
            "email_repair_backfill_apply_completed",
            run_id=run_id,
            duplicate_emails=summary.duplicate_emails,
            resolved_emails=summary.resolved_emails,
            loser_rows=summary.loser_rows,
        )
        return summary

    async def sync_teyca(self, *, batch_size: int) -> DuplicateEmailBackfillSummary:
        """Sync already-applied loser cleanup rows to Teyca."""
        summary = DuplicateEmailBackfillSummary()

        async with self.session_factory() as session:
            repair_repo = EmailRepairLogRepository(session)
            rows = await repair_repo.get_db_applied_batch(limit=max(1, batch_size))
            summary.loser_rows = len(rows)

            for row in rows:
                repair_id = int(row.id)
                loser_user_id = int(row.incoming_user_id)
                if row.winner_user_id is None or row.winner_subscriber_id is None:
                    raise DuplicateEmailBackfillError(
                        f"repair_id={repair_id} is missing winner metadata for Teyca sync"
                    )
                winner_user_id = int(row.winner_user_id)
                winner_subscriber_id = int(row.winner_subscriber_id)
                attempts = int(row.attempts) + 1

                try:
                    await self.teyca_client.update_pass_fields(
                        user_id=loser_user_id,
                        fields={"email": None, "key1": TEYCA_KEY1_BAD_EMAIL},
                    )
                    await repair_repo.mark_teyca_synced(
                        repair_id=repair_id,
                        winner_user_id=winner_user_id,
                        winner_subscriber_id=winner_subscriber_id,
                    )
                    summary.teyca_synced += 1
                    logger.info(
                        "email_repair_backfill_teyca_synced",
                        repair_id=repair_id,
                        loser_user_id=loser_user_id,
                        winner_user_id=winner_user_id,
                        winner_subscriber_id=winner_subscriber_id,
                    )
                except (TeycaAPIError, httpx.HTTPError) as exc:
                    status = await repair_repo.mark_retry(
                        repair_id=repair_id,
                        attempts=attempts,
                        error_text=str(exc),
                        max_attempts=EMAIL_REPAIR_MAX_ATTEMPTS,
                    )
                    if status == "manual_review":
                        summary.manual_review += 1
                    else:
                        summary.teyca_failed += 1
                    logger.error(
                        "email_repair_backfill_teyca_failed",
                        repair_id=repair_id,
                        loser_user_id=loser_user_id,
                        attempts=attempts,
                        status=status,
                        error=str(exc),
                        error_type=type(exc).__name__,
                    )

            await session.commit()

        logger.info(
            "email_repair_backfill_teyca_summary",
            loser_rows=summary.loser_rows,
            teyca_synced=summary.teyca_synced,
            teyca_failed=summary.teyca_failed,
            manual_review=summary.manual_review,
        )
        return summary


class DuplicateEmailBackfillError(RuntimeError):
    """Raised when one-time duplicate-email backfill cannot proceed safely."""


def build_duplicate_email_backfill() -> DuplicateEmailBackfill:
    """Build backfill helper from current settings and default clients."""
    settings = get_settings()
    return DuplicateEmailBackfill(
        settings=settings,
        session_factory=SessionLocal,
        listmonk_client=ListmonkSDKClient(settings),
        teyca_client=TeycaClient(settings),
    )
