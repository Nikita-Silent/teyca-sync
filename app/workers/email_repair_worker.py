"""Periodic worker: resolve duplicate emails via Listmonk truth."""

from __future__ import annotations

from dataclasses import dataclass

import httpx
import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog import contextvars as log_contextvars

from app.clients.listmonk import ListmonkClientError, ListmonkSDKClient
from app.clients.teyca import TeycaAPIError, TeycaClient
from app.config import Settings, get_settings
from app.db.session import SessionLocal
from app.repositories.email_repair_log import EmailRepairLogRepository
from app.repositories.listmonk_users import ListmonkUsersRepository
from app.repositories.users import UsersRepository

logger = structlog.get_logger()

EMAIL_REPAIR_MAX_ATTEMPTS = 3
TEYCA_KEY1_BAD_EMAIL = "bad email"


@dataclass(slots=True)
class EmailRepairMetrics:
    """Aggregated metrics for one duplicate-email repair run."""

    batch_size: int
    processed: int = 0
    synced: int = 0
    failed: int = 0
    manual_review: int = 0


@dataclass(slots=True)
class EmailRepairWorker:
    """Resolves duplicate-email conflicts scheduled by consumers."""

    settings: Settings
    session_factory: async_sessionmaker[AsyncSession]
    listmonk_client: ListmonkSDKClient
    teyca_client: TeycaClient

    async def run_once(self) -> int:
        """Process one batch of pending email repairs."""
        batch_size = max(1, self.settings.consent_sync_batch_size)
        metrics = EmailRepairMetrics(batch_size=batch_size)
        async with self.session_factory() as session:
            repair_repo = EmailRepairLogRepository(session)
            listmonk_repo = ListmonkUsersRepository(session)
            users_repo = UsersRepository(session)

            rows = await repair_repo.get_pending_batch(limit=batch_size)
            if not rows:
                logger.info("email_repair_no_pending_rows", batch_size=batch_size)
            for row in rows:
                metrics.processed += 1
                log_contextvars.bind_contextvars(
                    trace_id=row.trace_id,
                    source_event_id=row.source_event_id,
                    user_id=row.incoming_user_id,
                )
                try:
                    await repair_repo.mark_processing(repair_id=int(row.id))
                    await self._process_row(
                        row=row,
                        repair_repo=repair_repo,
                        listmonk_repo=listmonk_repo,
                        users_repo=users_repo,
                        metrics=metrics,
                    )
                finally:
                    log_contextvars.unbind_contextvars(
                        "trace_id",
                        "source_event_id",
                        "user_id",
                    )

            await session.commit()
        logger.info(
            "email_repair_metrics",
            batch_size=metrics.batch_size,
            processed=metrics.processed,
            synced=metrics.synced,
            failed=metrics.failed,
            manual_review=metrics.manual_review,
        )
        return metrics.processed

    async def _process_row(
        self,
        *,
        row: object,
        repair_repo: EmailRepairLogRepository,
        listmonk_repo: ListmonkUsersRepository,
        users_repo: UsersRepository,
        metrics: EmailRepairMetrics | None = None,
    ) -> None:
        repair_id = int(getattr(row, "id"))
        normalized_email = str(getattr(row, "normalized_email"))
        incoming_user_id = int(getattr(row, "incoming_user_id"))
        existing_user_id = int(getattr(row, "existing_user_id"))
        attempts = int(getattr(row, "attempts")) + 1

        try:
            subscriber = await self.listmonk_client.get_subscriber_by_email(email=normalized_email)
            if subscriber is None:
                raise EmailRepairResolutionError("subscriber_by_email returned no subscriber")

            winner_row = await listmonk_repo.get_by_subscriber_id(
                subscriber_id=subscriber.subscriber_id
            )
            if winner_row is None:
                raise EmailRepairResolutionError(
                    f"subscriber_id={subscriber.subscriber_id} is not mapped in listmonk_users"
                )

            winner_user_id = int(winner_row.user_id)
            candidate_user_ids = {incoming_user_id, existing_user_id}
            if winner_user_id not in candidate_user_ids:
                raise EmailRepairResolutionError(
                    "winner_user_id="
                    f"{winner_user_id} is outside repair pair {sorted(candidate_user_ids)}"
                )

            loser_user_id = (
                existing_user_id if winner_user_id == incoming_user_id else incoming_user_id
            )
            await users_repo.clear_email(user_id=loser_user_id)
            await listmonk_repo.clear_email(user_id=loser_user_id)
            await self.teyca_client.update_pass_fields(
                user_id=loser_user_id,
                fields={
                    "email": None,
                    "key1": TEYCA_KEY1_BAD_EMAIL,
                },
            )
            await repair_repo.mark_teyca_synced(
                repair_id=repair_id,
                winner_user_id=winner_user_id,
                winner_subscriber_id=subscriber.subscriber_id,
            )
            _inc(metrics, "synced")
            logger.info(
                "email_repair_synced",
                repair_id=repair_id,
                normalized_email=normalized_email,
                incoming_user_id=incoming_user_id,
                existing_user_id=existing_user_id,
                winner_user_id=winner_user_id,
                loser_user_id=loser_user_id,
                winner_subscriber_id=subscriber.subscriber_id,
            )
        except (
            EmailRepairResolutionError,
            ListmonkClientError,
            TeycaAPIError,
            httpx.HTTPError,
        ) as exc:
            status = await repair_repo.mark_retry(
                repair_id=repair_id,
                attempts=attempts,
                error_text=str(exc),
                max_attempts=EMAIL_REPAIR_MAX_ATTEMPTS,
            )
            _inc(metrics, status)
            logger.error(
                "email_repair_failed",
                repair_id=repair_id,
                normalized_email=normalized_email,
                incoming_user_id=incoming_user_id,
                existing_user_id=existing_user_id,
                attempts=attempts,
                status=status,
                error=str(exc),
                error_type=type(exc).__name__,
            )


class EmailRepairResolutionError(RuntimeError):
    """Raised when duplicate-email repair cannot be resolved automatically."""


def _inc(metrics: EmailRepairMetrics | None, attr: str) -> None:
    if metrics is not None:
        setattr(metrics, attr, getattr(metrics, attr) + 1)


def build_email_repair_worker() -> EmailRepairWorker:
    settings = get_settings()
    return EmailRepairWorker(
        settings=settings,
        session_factory=SessionLocal,
        listmonk_client=ListmonkSDKClient(settings),
        teyca_client=TeycaClient(settings),
    )
