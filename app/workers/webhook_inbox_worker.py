"""Polls webhook_inbox and dispatches CREATE/UPDATE/DELETE handlers (teyca-sync-8ib).

Replaces the RabbitMQ consumer (`run_queue_consumers.py`): the inbox row IS the
durable queue entry, so retry/backoff/dead-lettering reuse the same
claim/mark_done/mark_retry primitives as `external_call_outbox` instead of
RabbitMQ headers and dead-letter exchanges.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import structlog
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog import contextvars as log_contextvars

from app.clients.teyca import TeycaAPIError
from app.config import Settings, get_settings
from app.consumers.create_user import CreateConsumerDeps
from app.consumers.create_user import handle as handle_create
from app.consumers.delete_user import DeleteConsumerDeps
from app.consumers.delete_user import handle as handle_delete
from app.consumers.update_user import UpdateConsumerDeps
from app.consumers.update_user import handle as handle_update
from app.db.session import SessionLocal
from app.repositories.bonus_accrual import BonusAccrualRepository
from app.repositories.external_call_outbox import ExternalCallOutboxRepository
from app.repositories.listmonk_users import ListmonkUsersRepository
from app.repositories.merge_log import MergeLogRepository
from app.repositories.old_db import OldDBRepository
from app.repositories.users import UserLockNotAcquiredError, UsersRepository
from app.repositories.webhook_inbox import InboxClaim, WebhookInboxRepository

logger = structlog.get_logger()


@dataclass(slots=True)
class InboxWorkerMetrics:
    batch_size: int
    processed: int = 0
    done: int = 0
    retried: int = 0
    dead: int = 0


@dataclass(slots=True)
class WebhookInboxWorker:
    """Claims webhook_inbox rows and runs the matching CREATE/UPDATE/DELETE handler."""

    settings: Settings
    session_factory: async_sessionmaker[AsyncSession]
    old_db_repo: OldDBRepository
    worker_id: str

    async def _run_in_session(self, operation: Any) -> Any:
        async with self.session_factory() as session:
            try:
                result = await operation(session)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
        return result

    async def _claim_batch(self, *, limit: int) -> list[InboxClaim]:
        async def operation(session: AsyncSession) -> list[InboxClaim]:
            repo = WebhookInboxRepository(session)
            return await repo.claim_batch(limit=limit, worker_id=self.worker_id)

        return await self._run_in_session(operation)

    async def _mark_done(self, *, inbox_id: int) -> None:
        async def operation(session: AsyncSession) -> None:
            repo = WebhookInboxRepository(session)
            await repo.mark_done(inbox_id=inbox_id)

        await self._run_in_session(operation)

    async def _mark_retry(
        self,
        *,
        inbox_id: int,
        attempts: int,
        error_text: str,
        max_attempts: int,
        base_delay_ms: int,
        max_delay_ms: int,
    ) -> str:
        async def operation(session: AsyncSession) -> str:
            repo = WebhookInboxRepository(session)
            return await repo.mark_retry(
                inbox_id=inbox_id,
                attempts=attempts,
                error_text=error_text,
                max_attempts=max(1, max_attempts),
                base_delay_ms=max(1, base_delay_ms),
                max_delay_ms=max(1, max_delay_ms),
            )

        return str(await self._run_in_session(operation))

    async def _release_stale_claims(self) -> int:
        async def operation(session: AsyncSession) -> int:
            repo = WebhookInboxRepository(session)
            return await repo.release_stale_processing_claims(
                stale_after_seconds=self.settings.webhook_inbox_stale_claim_seconds
            )

        return int(await self._run_in_session(operation))

    async def run_once(self) -> int:
        stale_count = await self._release_stale_claims()
        if stale_count:
            logger.warning("webhook_inbox_stale_claims_released", count=stale_count)

        batch_size = max(1, self.settings.webhook_inbox_batch_size)
        metrics = InboxWorkerMetrics(batch_size=batch_size)
        claims = await self._claim_batch(limit=batch_size)
        if not claims:
            return 0

        for claim in claims:
            metrics.processed += 1
            log_contextvars.bind_contextvars(
                trace_id=claim.trace_id,
                source_event_id=claim.source_event_id,
                inbox_id=claim.id,
                event_type=claim.event_type,
            )
            try:
                await self._process_claim(claim=claim, metrics=metrics)
            finally:
                log_contextvars.unbind_contextvars(
                    "trace_id", "source_event_id", "inbox_id", "event_type"
                )

        logger.info(
            "webhook_inbox_metrics",
            batch_size=metrics.batch_size,
            processed=metrics.processed,
            done=metrics.done,
            retried=metrics.retried,
            dead=metrics.dead,
        )
        return metrics.processed

    async def _process_claim(self, *, claim: InboxClaim, metrics: InboxWorkerMetrics) -> None:
        wait_for_lock = claim.attempts > 0
        error_text = ""
        try:
            await self._dispatch(claim=claim, wait_for_lock=wait_for_lock)
            await self._mark_done(inbox_id=claim.id)
            metrics.done += 1
            return
        except UserLockNotAcquiredError as exc:
            error_text = f"user_lock_busy: user_id={exc.user_id}"
            log_event = "webhook_inbox_user_lock_busy"
            max_attempts = self.settings.webhook_inbox_lock_busy_retry_max_retries
            base_delay_ms = self.settings.webhook_inbox_lock_busy_retry_base_delay_ms
            max_delay_ms = self.settings.webhook_inbox_lock_busy_retry_max_delay_ms
        except TeycaAPIError as exc:
            error_text = str(exc)
            log_event = "webhook_inbox_teyca_error_retry_scheduled"
            if exc.is_rate_limited:
                max_attempts = self.settings.webhook_inbox_teyca_rate_limit_retry_max_retries
                base_delay_ms = self.settings.webhook_inbox_teyca_rate_limit_retry_base_delay_ms
                max_delay_ms = self.settings.webhook_inbox_teyca_rate_limit_retry_max_delay_ms
            else:
                max_attempts = self.settings.webhook_inbox_max_retries
                base_delay_ms = self.settings.webhook_inbox_retry_base_delay_ms
                max_delay_ms = self.settings.webhook_inbox_retry_max_delay_ms
        except ValidationError as exc:
            # teyca-sync-iil.5: a schema mismatch (e.g. a PassData field Teyca
            # sends in a shape we don't expect yet) is not transient — retrying
            # the same payload webhook_inbox_max_retries times just delays
            # noticing it. Go straight to dead so the row stays inspectable and
            # replayable (see replay tooling, teyca-sync-iil.6) once the schema
            # is fixed, instead of burning ~25 retries with backoff first.
            error_text = str(exc)
            log_event = "webhook_inbox_validation_failed"
            max_attempts = 1
            base_delay_ms = self.settings.webhook_inbox_retry_base_delay_ms
            max_delay_ms = self.settings.webhook_inbox_retry_max_delay_ms
        except Exception as exc:
            error_text = str(exc)
            log_event = "webhook_inbox_job_retry_scheduled"
            max_attempts = self.settings.webhook_inbox_max_retries
            base_delay_ms = self.settings.webhook_inbox_retry_base_delay_ms
            max_delay_ms = self.settings.webhook_inbox_retry_max_delay_ms

        status = await self._mark_retry(
            inbox_id=claim.id,
            attempts=claim.attempts + 1,
            error_text=error_text,
            max_attempts=max_attempts,
            base_delay_ms=base_delay_ms,
            max_delay_ms=max_delay_ms,
        )
        if status == "dead":
            metrics.dead += 1
        else:
            metrics.retried += 1
        logger.error(
            log_event,
            inbox_id=claim.id,
            event_type=claim.event_type,
            attempts=claim.attempts + 1,
            status=status,
            error=error_text,
        )

    async def _dispatch(self, *, claim: InboxClaim, wait_for_lock: bool) -> None:
        async with self.session_factory() as session:
            try:
                if claim.event_type == "CREATE":
                    deps_create = CreateConsumerDeps(
                        settings=self.settings,
                        session=session,
                        users_repo=UsersRepository(session),
                        listmonk_repo=ListmonkUsersRepository(session),
                        outbox_repo=ExternalCallOutboxRepository(session),
                        merge_repo=MergeLogRepository(session),
                        old_db_repo=self.old_db_repo,
                    )
                    await handle_create(
                        claim.payload, deps=deps_create, wait_for_lock=wait_for_lock
                    )
                elif claim.event_type == "UPDATE":
                    deps_update = UpdateConsumerDeps(
                        settings=self.settings,
                        session=session,
                        users_repo=UsersRepository(session),
                        listmonk_repo=ListmonkUsersRepository(session),
                        outbox_repo=ExternalCallOutboxRepository(session),
                        merge_repo=MergeLogRepository(session),
                        old_db_repo=self.old_db_repo,
                    )
                    await handle_update(
                        claim.payload, deps=deps_update, wait_for_lock=wait_for_lock
                    )
                elif claim.event_type == "DELETE":
                    deps_delete = DeleteConsumerDeps(
                        users_repo=UsersRepository(session),
                        listmonk_repo=ListmonkUsersRepository(session),
                        merge_repo=MergeLogRepository(session),
                        bonus_accrual_repo=BonusAccrualRepository(session),
                        outbox_repo=ExternalCallOutboxRepository(session),
                    )
                    await handle_delete(
                        claim.payload, deps=deps_delete, wait_for_lock=wait_for_lock
                    )
                else:
                    raise ValueError(f"Unsupported event type: {claim.event_type}")
                await session.commit()
            except Exception:
                await session.rollback()
                raise


def build_webhook_inbox_worker(*, worker_id_prefix: str = "webhook-inbox") -> WebhookInboxWorker:
    settings = get_settings()
    return WebhookInboxWorker(
        settings=settings,
        session_factory=SessionLocal,
        old_db_repo=OldDBRepository(
            settings.export_db_url,
            request_timeout_seconds=settings.export_db_request_timeout_seconds,
        ),
        worker_id=f"{worker_id_prefix}:{uuid4().hex}",
    )
