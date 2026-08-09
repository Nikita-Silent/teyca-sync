"""Durable dispatcher for external Listmonk/Teyca side effects."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, cast
from uuid import uuid4

import httpx
import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog import contextvars as log_contextvars

from app.clients.listmonk import (
    ListmonkClientError,
    ListmonkSDKClient,
    SubscriberState,
    httpx2_network_exceptions,
    httpx2_status_exceptions,
)
from app.clients.teyca import (
    BonusOperation,
    TeycaAPIError,
    TeycaClient,
    TeycaRateLimitBusyError,
    _build_budget_limits,
    build_teyca_client,
)
from app.config import Settings, get_settings
from app.db.session import SessionLocal
from app.repositories.bonus_accrual import BonusAccrualRepository
from app.repositories.email_repair_log import EmailRepairLogRepository
from app.repositories.external_call_outbox import (
    OUTBOX_OP_LISTMONK_DELETE,
    OUTBOX_OP_LISTMONK_UPSERT,
    OUTBOX_OP_MERGE_FINALIZE,
    OUTBOX_OP_TEYCA_BLOCK_CONSENT,
    OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL,
    OUTBOX_OP_TEYCA_EMAIL_REPAIR_SYNC,
    ExternalCallOutboxRepository,
    OutboxClaim,
)
from app.repositories.listmonk_users import (
    DuplicateListmonkSubscriberIdError,
    DuplicateListmonkUserEmailError,
    ListmonkUsersRepository,
)
from app.repositories.merge_log import MergeLogRepository
from app.repositories.teyca_call_budget import TeycaCallBudgetRepository
from app.repositories.users import UsersRepository

logger = structlog.get_logger()

BONUS_REASON_EMAIL_CONSENT = "email_consent"
TEYCA_KEY1_CONFIRMED = "confirmed"
TEYCA_KEY1_BLOCKED = "blocked"
TEYCA_KEY1_BAD_EMAIL = "bad email"
TEYCA_KEY6_BUGS = "bugs"

LISTMONK_OUTBOX_OPERATIONS = (
    OUTBOX_OP_LISTMONK_UPSERT,
    OUTBOX_OP_LISTMONK_DELETE,
)
INVALID_EMAIL_OUTBOX_OPERATIONS = (OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL,)
MERGE_OUTBOX_OPERATIONS = (OUTBOX_OP_MERGE_FINALIZE,)
CONSENT_BLOCK_OUTBOX_OPERATIONS = (OUTBOX_OP_TEYCA_BLOCK_CONSENT,)
EMAIL_REPAIR_SYNC_OUTBOX_OPERATIONS = (OUTBOX_OP_TEYCA_EMAIL_REPAIR_SYNC,)
DEFAULT_OUTBOX_OPERATIONS = (
    *LISTMONK_OUTBOX_OPERATIONS,
    *INVALID_EMAIL_OUTBOX_OPERATIONS,
    *MERGE_OUTBOX_OPERATIONS,
    *CONSENT_BLOCK_OUTBOX_OPERATIONS,
    *EMAIL_REPAIR_SYNC_OUTBOX_OPERATIONS,
)


@dataclass(slots=True)
class ListmonkUpsertOutcome:
    """Result of persisting a listmonk_upsert mapping.

    `duplicate_reason` set means the mapping conflicted (subscriber_id or
    email already claimed by another user) and the claim must retry/dead,
    never mark_done (teyca-sync-kia) — the discrepancy would otherwise be
    silently lost until the next reconcile.
    """

    mapped: bool
    duplicate_reason: str | None = None


@dataclass(slots=True)
class ExternalDispatcherMetrics:
    batch_size: int
    processed: int = 0
    done: int = 0
    retried: int = 0
    dead: int = 0
    skipped: int = 0


@dataclass(slots=True)
class ExternalDispatcherWorker:
    """Polls external_call_outbox and executes side effects after DB commit."""

    settings: Settings
    session_factory: async_sessionmaker[AsyncSession]
    listmonk_client: ListmonkSDKClient
    teyca_client: TeycaClient
    worker_id: str
    operations: tuple[str, ...]
    # Scratch state for the current run_once() batch only (teyca-sync-axq): lets a
    # listmonk_upsert claim fold an already-claimed merge_finalize claim for the same
    # user into one combined Teyca call pair instead of processing both separately.
    _pending_merge_claims: dict[int, OutboxClaim] = field(default_factory=dict, repr=False)
    _combined_merge_ids: set[int] = field(default_factory=set, repr=False)

    async def _run_in_session(
        self,
        operation: Callable[[AsyncSession], Awaitable[Any]],
    ) -> Any:
        async with self.session_factory() as session:
            try:
                result = await operation(session)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
        return result

    async def _claim_batch(self, *, limit: int) -> list[OutboxClaim]:
        async def operation(session: AsyncSession) -> list[OutboxClaim]:
            repo = ExternalCallOutboxRepository(session)
            return await repo.claim_batch(
                operations=list(self.operations),
                limit=limit,
                worker_id=self.worker_id,
            )

        return await self._run_in_session(operation)

    async def _mark_done(self, *, outbox_id: int, payload: dict[str, Any] | None = None) -> None:
        async def operation(session: AsyncSession) -> None:
            repo = ExternalCallOutboxRepository(session)
            await repo.mark_done(outbox_id=outbox_id, payload=payload)

        await self._run_in_session(operation)

    async def _save_progress(
        self,
        *,
        outbox_id: int,
        payload: dict[str, Any],
        error_text: str | None = None,
    ) -> None:
        async def operation(session: AsyncSession) -> None:
            repo = ExternalCallOutboxRepository(session)
            await repo.save_progress(outbox_id=outbox_id, payload=payload, error_text=error_text)

        await self._run_in_session(operation)

    async def _mark_retry(self, *, outbox_id: int, attempts: int, error_text: str) -> str:
        async def operation(session: AsyncSession) -> str:
            repo = ExternalCallOutboxRepository(session)
            return await repo.mark_retry(
                outbox_id=outbox_id,
                attempts=attempts,
                error_text=error_text,
                max_attempts=max(1, self.settings.external_dispatcher_max_retries),
                base_delay_ms=max(1, self.settings.external_dispatcher_retry_base_delay_ms),
                max_delay_ms=max(1, self.settings.external_dispatcher_retry_max_delay_ms),
            )

        return str(await self._run_in_session(operation))

    async def _defer_rate_limit_busy(
        self,
        *,
        outbox_id: int,
        attempts: int,
        wait_seconds: float,
        error_text: str,
    ) -> str:
        async def operation(session: AsyncSession) -> str:
            repo = ExternalCallOutboxRepository(session)
            return await repo.defer(
                outbox_id=outbox_id,
                attempts=attempts,
                delay_seconds=max(wait_seconds, 0.0),
                error_text=error_text,
                max_attempts=max(1, self.settings.external_dispatcher_max_retries),
            )

        return str(await self._run_in_session(operation))

    def _teyca_rate_limit_max_wait_seconds(self) -> float:
        configured = float(
            getattr(self.settings, "external_dispatcher_teyca_rate_limit_max_wait_seconds", 0.0)
        )
        return max(0.0, configured)

    async def _release_stale_claims(self) -> int:
        stale_seconds = float(
            getattr(self.settings, "external_dispatcher_stale_claim_seconds", 300.0)
        )

        async def operation(session: AsyncSession) -> int:
            repo = ExternalCallOutboxRepository(session)
            return await repo.release_stale_processing_claims(stale_after_seconds=stale_seconds)

        return int(await self._run_in_session(operation))

    async def _teyca_budget_remaining(self) -> int:
        async def operation(session: AsyncSession) -> int:
            repo = TeycaCallBudgetRepository(session)
            return await repo.get_remaining(limits=_build_budget_limits(self.settings))

        return int(await self._run_in_session(operation))

    async def run_once(self) -> int:
        stale_count = await self._release_stale_claims()
        if stale_count:
            logger.warning("external_dispatcher_stale_claims_released", count=stale_count)
        batch_size = max(1, self.settings.external_dispatcher_batch_size)
        metrics = ExternalDispatcherMetrics(batch_size=batch_size)

        # Cap the claim itself by the remaining Teyca budget (teyca-sync-3al):
        # an exhausted budget means rows are never even taken from the outbox,
        # rather than claimed and then blocked waiting for a slot to free up.
        remaining_budget = await self._teyca_budget_remaining()
        effective_limit = min(batch_size, remaining_budget)
        if effective_limit <= 0:
            logger.info(
                "external_dispatcher_budget_exhausted",
                batch_size=batch_size,
                remaining_budget=remaining_budget,
            )
            return 0

        claims = await self._claim_batch(limit=effective_limit)
        if not claims:
            logger.info("external_dispatcher_no_pending_jobs", batch_size=batch_size)
            return 0

        self._pending_merge_claims = {
            claim.user_id: claim for claim in claims if claim.operation == OUTBOX_OP_MERGE_FINALIZE
        }
        self._combined_merge_ids = set()
        try:
            for claim in claims:
                if claim.id in self._combined_merge_ids:
                    # Already executed as part of a combined listmonk_upsert claim above.
                    metrics.processed += 1
                    metrics.done += 1
                    continue
                metrics.processed += 1
                log_contextvars.bind_contextvars(
                    trace_id=claim.trace_id,
                    source_event_id=claim.source_event_id,
                    user_id=claim.user_id,
                    queue_name=claim.queue_name,
                    outbox_id=claim.id,
                    outbox_operation=claim.operation,
                )
                try:
                    await self._process_claim(claim=claim, metrics=metrics)
                finally:
                    log_contextvars.unbind_contextvars(
                        "trace_id",
                        "source_event_id",
                        "user_id",
                        "queue_name",
                        "outbox_id",
                        "outbox_operation",
                    )
        finally:
            self._pending_merge_claims = {}
            self._combined_merge_ids = set()

        logger.info(
            "external_dispatcher_metrics",
            batch_size=metrics.batch_size,
            processed=metrics.processed,
            done=metrics.done,
            retried=metrics.retried,
            dead=metrics.dead,
            skipped=metrics.skipped,
        )
        return metrics.processed

    def _claim_handler(
        self, *, operation: str
    ) -> Callable[..., Awaitable[None]] | None:
        handlers: dict[str, Callable[..., Awaitable[None]]] = {
            OUTBOX_OP_LISTMONK_UPSERT: self._process_listmonk_upsert,
            OUTBOX_OP_LISTMONK_DELETE: self._process_listmonk_delete,
            OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL: self._process_invalid_email_block,
            OUTBOX_OP_MERGE_FINALIZE: self._process_merge_finalize,
            OUTBOX_OP_TEYCA_BLOCK_CONSENT: self._process_consent_block,
            OUTBOX_OP_TEYCA_EMAIL_REPAIR_SYNC: self._process_email_repair_sync,
        }
        return handlers.get(operation)

    async def _process_claim(
        self,
        *,
        claim: OutboxClaim,
        metrics: ExternalDispatcherMetrics,
    ) -> None:
        try:
            handler = self._claim_handler(operation=claim.operation)
            if handler is None:
                raise RuntimeError(f"Unsupported outbox operation: {claim.operation}")
            await handler(claim=claim, metrics=metrics)
        except TeycaRateLimitBusyError as exc:
            status = await self._defer_rate_limit_busy(
                outbox_id=claim.id,
                attempts=claim.attempts + 1,
                wait_seconds=exc.wait_seconds,
                error_text=str(exc),
            )
            if status == "dead":
                metrics.dead += 1
            else:
                metrics.retried += 1
            logger.warning(
                "external_dispatcher_job_rate_limit_deferred",
                outbox_id=claim.id,
                operation=claim.operation,
                attempts=claim.attempts + 1,
                status=status,
                wait_seconds=round(exc.wait_seconds, 3),
                max_wait_seconds=round(exc.max_wait_seconds, 3),
                backend=exc.backend,
            )
        except (
            ListmonkClientError,
            TeycaAPIError,
            httpx.HTTPError,
            RuntimeError,
            *httpx2_network_exceptions(),
            *httpx2_status_exceptions(),
        ) as exc:
            status = await self._mark_retry(
                outbox_id=claim.id,
                attempts=claim.attempts + 1,
                error_text=str(exc),
            )
            if status == "dead":
                metrics.dead += 1
            else:
                metrics.retried += 1
            logger.error(
                "external_dispatcher_job_retry_scheduled",
                outbox_id=claim.id,
                operation=claim.operation,
                attempts=claim.attempts + 1,
                status=status,
                error=str(exc),
                error_type=type(exc).__name__,
            )

    async def _process_listmonk_upsert(
        self,
        *,
        claim: OutboxClaim,
        metrics: ExternalDispatcherMetrics,
    ) -> None:
        if not await self._user_exists(user_id=claim.user_id):
            await self._mark_done(outbox_id=claim.id, payload=claim.payload)
            metrics.skipped += 1
            logger.info("external_dispatcher_listmonk_upsert_user_missing", outbox_id=claim.id)
            return
        email = _payload_text(claim.payload, key="email")
        attributes = dict(claim.payload.get("attributes") or {})
        list_ids = _payload_int_list(claim.payload, key="list_ids")
        subscriber_id = _payload_optional_int(claim.payload, key="subscriber_id")
        event_type = _payload_text(claim.payload, key="event_type") or "UPDATE"

        state = await self.listmonk_client.upsert_subscriber(
            email=email,
            list_ids=list_ids,
            attributes=attributes,
            subscriber_id=subscriber_id,
        )
        outcome = await self._apply_listmonk_upsert_success(
            claim=claim,
            state=state,
            event_type=event_type,
        )
        if outcome.duplicate_reason is not None:
            status = await self._mark_retry(
                outbox_id=claim.id,
                attempts=claim.attempts + 1,
                error_text=outcome.duplicate_reason,
            )
            if status == "dead":
                metrics.dead += 1
            else:
                metrics.retried += 1
            logger.error(
                "external_dispatcher_listmonk_upsert_duplicate_retry_scheduled",
                outbox_id=claim.id,
                attempts=claim.attempts + 1,
                status=status,
                error=outcome.duplicate_reason,
            )
            return
        if outcome.mapped:
            await self._accrue_consent_bonus_if_needed(user_id=claim.user_id)
        await self._mark_done(outbox_id=claim.id)
        metrics.done += 1
        logger.info(
            "external_dispatcher_listmonk_upsert_done",
            outbox_id=claim.id,
            subscriber_id=state.subscriber_id,
            status=state.status,
            list_ids=state.list_ids,
        )

    async def _apply_listmonk_upsert_success(
        self,
        *,
        claim: OutboxClaim,
        state: SubscriberState,
        event_type: str,
    ) -> ListmonkUpsertOutcome:
        """Persist the restored mapping. `mapped=True` only when the mapping was
        written without conflict — the only case eligible for a consent bonus
        (Р5). A conflict sets `duplicate_reason` so the caller retries/deads
        the claim instead of marking it done (teyca-sync-kia)."""

        async def operation(session: AsyncSession) -> ListmonkUpsertOutcome:
            users_repo = UsersRepository(session)
            listmonk_repo = ListmonkUsersRepository(session)
            email_repair_repo = EmailRepairLogRepository(session)

            current_user = await users_repo.get_by_user_id(user_id=claim.user_id)
            if current_user is None:
                return ListmonkUpsertOutcome(mapped=False)
            try:
                await listmonk_repo.upsert(
                    user_id=claim.user_id,
                    subscriber_id=state.subscriber_id,
                    email=_payload_text(claim.payload, key="email"),
                    status=state.status,
                    list_ids=state.list_ids,
                    attributes=dict(claim.payload.get("attributes") or {}),
                )
            except DuplicateListmonkSubscriberIdError as exc:
                logger.error(
                    "external_dispatcher_duplicate_subscriber_id",
                    outbox_id=claim.id,
                    user_id=claim.user_id,
                    subscriber_id=exc.subscriber_id,
                    existing_user_ids=exc.user_ids,
                )
                return ListmonkUpsertOutcome(mapped=False, duplicate_reason=str(exc))
            except DuplicateListmonkUserEmailError as exc:
                for existing_user_id in exc.existing_user_ids:
                    await email_repair_repo.create_pending(
                        normalized_email=exc.normalized_email,
                        incoming_user_id=claim.user_id,
                        existing_user_id=existing_user_id,
                        source_event_type=event_type,
                        source_event_id=claim.source_event_id,
                        trace_id=claim.trace_id,
                    )
                logger.error(
                    "external_dispatcher_duplicate_email_scheduled",
                    outbox_id=claim.id,
                    user_id=claim.user_id,
                    email=exc.normalized_email,
                    existing_user_ids=exc.existing_user_ids,
                )
                return ListmonkUpsertOutcome(mapped=False, duplicate_reason=str(exc))
            await listmonk_repo.set_consent_pending(user_id=claim.user_id)
            return ListmonkUpsertOutcome(mapped=True)

        return cast(ListmonkUpsertOutcome, await self._run_in_session(operation))

    async def _process_listmonk_delete(
        self,
        *,
        claim: OutboxClaim,
        metrics: ExternalDispatcherMetrics,
    ) -> None:
        subscriber_id = _payload_optional_int(claim.payload, key="subscriber_id")
        if subscriber_id is None:
            await self._mark_done(outbox_id=claim.id)
            metrics.skipped += 1
            logger.info("external_dispatcher_listmonk_delete_skipped", outbox_id=claim.id)
            return
        await self.listmonk_client.delete_subscriber(subscriber_id=subscriber_id)
        await self._mark_done(outbox_id=claim.id)
        metrics.done += 1
        logger.info(
            "external_dispatcher_listmonk_delete_done",
            outbox_id=claim.id,
            subscriber_id=subscriber_id,
        )

    async def _process_invalid_email_block(
        self,
        *,
        claim: OutboxClaim,
        metrics: ExternalDispatcherMetrics,
    ) -> None:
        if not await self._user_exists(user_id=claim.user_id):
            await self._mark_done(outbox_id=claim.id, payload=claim.payload)
            metrics.skipped += 1
            logger.info("external_dispatcher_invalid_email_block_user_missing", outbox_id=claim.id)
            return
        status = _payload_text(claim.payload, key="status") or "blocked"
        await self._send_teyca_key_if_changed(user_id=claim.user_id, key="key1", value=status)
        await self._apply_invalid_email_block_success(user_id=claim.user_id, status=status)
        await self._mark_done(outbox_id=claim.id)
        metrics.done += 1
        logger.info(
            "external_dispatcher_invalid_email_block_done",
            outbox_id=claim.id,
            status=status,
        )

    async def _apply_invalid_email_block_success(self, *, user_id: int, status: str) -> None:
        async def operation(session: AsyncSession) -> None:
            listmonk_repo = ListmonkUsersRepository(session)
            current = await listmonk_repo.get_by_user_id(user_id=user_id)
            if current is None:
                return
            await listmonk_repo.mark_checked(
                user_id=user_id,
                pending=False,
                confirmed=False,
                status=status,
            )

        await self._run_in_session(operation)

    async def _process_consent_block(
        self,
        *,
        claim: OutboxClaim,
        metrics: ExternalDispatcherMetrics,
    ) -> None:
        """Deliver key1=blocked to Teyca for a consent-sync unsubscribe
        (teyca-sync-dd2.1). Lowest dispatch priority (`claim_batch`) so this
        6362-record backlog never competes with real-time work — it only
        drains whatever Teyca call budget is left over. `_send_teyca_key_if_changed`
        skips the call entirely if this user's key1 already reads `blocked`."""
        if not await self._user_exists(user_id=claim.user_id):
            await self._mark_done(outbox_id=claim.id, payload=claim.payload)
            metrics.skipped += 1
            logger.info("external_dispatcher_consent_block_user_missing", outbox_id=claim.id)
            return
        status = _payload_text(claim.payload, key="status") or TEYCA_KEY1_BLOCKED
        await self._send_teyca_key_if_changed(user_id=claim.user_id, key="key1", value=status)
        await self._apply_invalid_email_block_success(user_id=claim.user_id, status=status)
        await self._mark_done(outbox_id=claim.id)
        metrics.done += 1
        logger.info(
            "external_dispatcher_consent_block_done",
            outbox_id=claim.id,
            status=status,
        )

    async def _process_email_repair_sync(
        self,
        *,
        claim: OutboxClaim,
        metrics: ExternalDispatcherMetrics,
    ) -> None:
        """Deliver the Р5/Р6 winner/loser Teyca update for one duplicate-email
        group (teyca-sync-y1c). Seeded by run_email_duplicate_policy_backfill's
        --sync-teyca instead of calling Teyca directly, so this one-time
        cleanup backlog drains under the same budget-aware pacing as
        real-time work instead of a tight loop that blows through the
        hourly window in one shot. `claim.user_id` is the loser."""
        repair_id = _payload_optional_int(claim.payload, key="repair_id")
        winner_user_id = _payload_optional_int(claim.payload, key="winner_user_id")
        if repair_id is None or winner_user_id is None:
            raise RuntimeError(
                f"email_repair_sync payload missing repair_id/winner_user_id: {claim.payload}"
            )
        winner_subscriber_id = _payload_optional_int(claim.payload, key="winner_subscriber_id")
        mark_bad_email = bool(claim.payload.get("mark_bad_email", True))
        loser_user_id = claim.user_id

        if not await self._user_exists(user_id=loser_user_id):
            await self._mark_email_repair_synced(
                repair_id=repair_id,
                winner_user_id=winner_user_id,
                winner_subscriber_id=winner_subscriber_id,
            )
            await self._mark_done(outbox_id=claim.id)
            metrics.skipped += 1
            logger.info(
                "external_dispatcher_email_repair_sync_user_missing",
                outbox_id=claim.id,
                repair_id=repair_id,
            )
            return

        await self.teyca_client.update_pass_fields(
            user_id=winner_user_id,
            fields={"key6": TEYCA_KEY6_BUGS},
            rate_limit_max_wait_seconds=self._teyca_rate_limit_max_wait_seconds(),
        )
        loser_fields: dict[str, Any] = {"email": None, "key6": TEYCA_KEY6_BUGS}
        if mark_bad_email:
            loser_fields["key1"] = TEYCA_KEY1_BAD_EMAIL
        await self.teyca_client.update_pass_fields(
            user_id=loser_user_id,
            fields=loser_fields,
            rate_limit_max_wait_seconds=self._teyca_rate_limit_max_wait_seconds(),
        )
        await self._mark_email_repair_synced(
            repair_id=repair_id,
            winner_user_id=winner_user_id,
            winner_subscriber_id=winner_subscriber_id,
        )
        await self._mark_done(outbox_id=claim.id)
        metrics.done += 1
        logger.info(
            "external_dispatcher_email_repair_sync_done",
            outbox_id=claim.id,
            repair_id=repair_id,
            winner_user_id=winner_user_id,
            loser_user_id=loser_user_id,
            mark_bad_email=mark_bad_email,
        )

    async def _mark_email_repair_synced(
        self,
        *,
        repair_id: int,
        winner_user_id: int,
        winner_subscriber_id: int | None,
    ) -> None:
        async def operation(session: AsyncSession) -> None:
            repair_repo = EmailRepairLogRepository(session)
            await repair_repo.mark_teyca_synced(
                repair_id=repair_id,
                winner_user_id=winner_user_id,
                winner_subscriber_id=winner_subscriber_id,
            )

        await self._run_in_session(operation)

    async def _process_merge_finalize(
        self,
        *,
        claim: OutboxClaim,
        metrics: ExternalDispatcherMetrics,
    ) -> None:
        # Guard against a same-batch listmonk_upsert claim combining into this
        # claim after it has already been (or is being) processed on its own.
        self._pending_merge_claims.pop(claim.user_id, None)
        payload = _normalize_merge_payload(claim.payload)
        if await self._merge_already_logged(user_id=claim.user_id):
            payload["merge_logged"] = True
            await self._mark_done(outbox_id=claim.id, payload=payload)
            metrics.skipped += 1
            logger.info("external_dispatcher_merge_already_done", outbox_id=claim.id)
            return
        if not await self._user_exists(user_id=claim.user_id):
            await self._mark_done(outbox_id=claim.id, payload=payload)
            metrics.skipped += 1
            logger.info("external_dispatcher_merge_user_missing", outbox_id=claim.id)
            return

        old_bonus_value = _payload_optional_float(payload, key="old_bonus_value")
        if not payload["bonus_done"] and old_bonus_value is not None and old_bonus_value > 0:
            await self.teyca_client.accrue_bonuses(
                user_id=claim.user_id,
                bonuses=[BonusOperation.one_shot(value=str(old_bonus_value))],
                rate_limit_max_wait_seconds=self._teyca_rate_limit_max_wait_seconds(),
            )
            payload["bonus_done"] = True
            await self._save_progress(outbox_id=claim.id, payload=payload)

        if not payload["key2_done"]:
            await self._send_teyca_key_if_changed(
                user_id=claim.user_id,
                key="key2",
                value=_payload_text(payload, key="merge_key2_value") or "",
            )
            payload["key2_done"] = True
            await self._save_progress(outbox_id=claim.id, payload=payload)

        if not payload["merge_logged"]:
            await self._write_merge_log(
                user_id=claim.user_id,
                source_event_type=_payload_text(payload, key="source_event_type") or "UPDATE",
                source_event_id=claim.source_event_id,
                trace_id=claim.trace_id,
            )
            payload["merge_logged"] = True

        await self._mark_done(outbox_id=claim.id, payload=payload)
        metrics.done += 1
        logger.info(
            "external_dispatcher_merge_finalize_done",
            outbox_id=claim.id,
            bonus_done=payload["bonus_done"],
            key2_done=payload["key2_done"],
            merge_logged=payload["merge_logged"],
        )

    async def _teyca_key_last_sent(
        self, *, user_id: int, key: Literal["key1", "key2"]
    ) -> str | None:
        async def read_operation(session: AsyncSession) -> str | None:
            users_repo = UsersRepository(session)
            return await users_repo.get_teyca_key_value(user_id=user_id, key=key)

        return await self._run_in_session(read_operation)

    async def _set_teyca_key_value(
        self, *, user_id: int, key: Literal["key1", "key2"], value: str
    ) -> None:
        async def write_operation(session: AsyncSession) -> None:
            users_repo = UsersRepository(session)
            await users_repo.set_teyca_key_value(user_id=user_id, key=key, value=value)

        await self._run_in_session(write_operation)

    async def _send_teyca_key_if_changed(
        self, *, user_id: int, key: Literal["key1", "key2"], value: str
    ) -> None:
        """Call Teyca update_pass_fields only if this value wasn't already sent.

        Resending the same key1/key2 wastes rate limit budget (teyca-sync-i6r
        burned the daily limit resending key1=blocked to 6362 already-blocked
        subscribers) — skip the call when the last value we actually sent
        matches.
        """
        last_sent = await self._teyca_key_last_sent(user_id=user_id, key=key)
        if last_sent == value:
            logger.info(
                "external_dispatcher_teyca_key_unchanged",
                user_id=user_id,
                key=key,
                value=value,
            )
            return

        await self.teyca_client.update_pass_fields(
            user_id=user_id,
            fields={key: value},
            rate_limit_max_wait_seconds=self._teyca_rate_limit_max_wait_seconds(),
        )
        await self._set_teyca_key_value(user_id=user_id, key=key, value=value)

    async def _user_exists(self, *, user_id: int) -> bool:
        async def operation(session: AsyncSession) -> bool:
            users_repo = UsersRepository(session)
            return await users_repo.get_by_user_id(user_id=user_id) is not None

        return bool(await self._run_in_session(operation))

    async def _merge_already_logged(self, *, user_id: int) -> bool:
        async def operation(session: AsyncSession) -> bool:
            merge_repo = MergeLogRepository(session)
            return await merge_repo.exists(user_id=user_id)

        return bool(await self._run_in_session(operation))

    async def _write_merge_log(
        self,
        *,
        user_id: int,
        source_event_type: str,
        source_event_id: str | None,
        trace_id: str | None,
    ) -> None:
        async def operation(session: AsyncSession) -> None:
            merge_repo = MergeLogRepository(session)
            if await merge_repo.exists(user_id=user_id):
                return
            await merge_repo.create(
                user_id=user_id,
                source_event_type=source_event_type,
                source_event_id=source_event_id,
                trace_id=trace_id,
            )

        await self._run_in_session(operation)

    async def _accrue_consent_bonus_if_needed(self, *, user_id: int) -> None:
        """Pay the consent bonus once per user (Р4а/Р5).

        Triggered only from a successfully applied CRM-driven `listmonk_upsert` —
        never from reconcile or bulk subscriber-id recompute, which restore the
        mapping without any evidence of a fresh CRM event. Idempotent across
        retries via `bonus_accrual_log` keyed by `email_consent:{user_id}`, same
        key the previous consent-sync-driven accrual used.
        """
        idempotency_key = f"{BONUS_REASON_EMAIL_CONSENT}:{user_id}"
        await self._bonus_reserve(
            user_id=user_id,
            idempotency_key=idempotency_key,
            payload=_initial_bonus_payload(),
        )
        saved_payload = await self._bonus_get_payload(idempotency_key=idempotency_key)
        if saved_payload is None:
            logger.error(
                "external_dispatcher_consent_bonus_operation_missing",
                user_id=user_id,
                idempotency_key=idempotency_key,
            )
            return
        payload = _normalize_bonus_payload(saved_payload)

        merge_claim = self._pending_merge_claims.get(user_id)
        if merge_claim is not None and (not payload["bonus_done"] or not payload["key1_done"]):
            self._combined_merge_ids.add(merge_claim.id)
            await self._finalize_consent_and_merge(
                user_id=user_id,
                idempotency_key=idempotency_key,
                payload=payload,
                merge_claim=merge_claim,
            )
            return

        if not payload["bonus_done"]:
            await self.teyca_client.accrue_bonuses(
                user_id=user_id,
                bonuses=[BonusOperation.one_shot(value=str(self.settings.consent_bonus_amount))],
                rate_limit_max_wait_seconds=self._teyca_rate_limit_max_wait_seconds(),
            )
            payload["bonus_done"] = True
            await self._bonus_save_progress(idempotency_key=idempotency_key, payload=payload)

        if not payload["key1_done"]:
            await self._send_teyca_key_if_changed(
                user_id=user_id, key="key1", value=TEYCA_KEY1_CONFIRMED
            )
            payload["key1_done"] = True
            await self._bonus_save_progress(idempotency_key=idempotency_key, payload=payload)

        await self._bonus_mark_done(idempotency_key=idempotency_key, payload=payload)
        logger.info(
            "external_dispatcher_consent_bonus_done",
            user_id=user_id,
            bonus_done=payload["bonus_done"],
            key1_done=payload["key1_done"],
        )

    async def _finalize_consent_and_merge(
        self,
        *,
        user_id: int,
        idempotency_key: str,
        payload: dict[str, Any],
        merge_claim: OutboxClaim,
    ) -> None:
        """Combine consent-bonus and merge-finalize Teyca calls for a new client.

        POST /v1/{token}/passes/{user_id}/bonuses accepts an array and
        PUT /v1/{token}/passes/{user_id} accepts an arbitrary field set, so both
        flows can share one bonus call and one field call — 2 Teyca requests for
        a new client instead of 4 (teyca-sync-axq). There is no batch endpoint for
        card changes; combining fields is the only way to cut call count.
        """
        merge_payload = _normalize_merge_payload(merge_claim.payload)

        include_consent_bonus = not payload["bonus_done"]
        old_bonus_value = _payload_optional_float(merge_claim.payload, key="old_bonus_value")
        include_merge_bonus = not merge_payload["bonus_done"] and (
            old_bonus_value is not None and old_bonus_value > 0
        )
        bonuses: list[BonusOperation] = []
        if include_consent_bonus:
            bonuses.append(BonusOperation.one_shot(value=str(self.settings.consent_bonus_amount)))
        if include_merge_bonus:
            bonuses.append(BonusOperation.one_shot(value=str(old_bonus_value)))

        if bonuses:
            await self.teyca_client.accrue_bonuses(
                user_id=user_id,
                bonuses=bonuses,
                rate_limit_max_wait_seconds=self._teyca_rate_limit_max_wait_seconds(),
            )
        if include_consent_bonus:
            payload["bonus_done"] = True
            await self._bonus_save_progress(idempotency_key=idempotency_key, payload=payload)
        if include_merge_bonus:
            merge_payload["bonus_done"] = True
            await self._save_progress(outbox_id=merge_claim.id, payload=merge_payload)

        include_key1 = not payload["key1_done"]
        include_key2 = not merge_payload["key2_done"]
        fields: dict[str, object] = {}
        if include_key1:
            last_sent = await self._teyca_key_last_sent(user_id=user_id, key="key1")
            if last_sent != TEYCA_KEY1_CONFIRMED:
                fields["key1"] = TEYCA_KEY1_CONFIRMED
        if include_key2:
            key2_value = merge_payload["merge_key2_value"] or ""
            last_sent = await self._teyca_key_last_sent(user_id=user_id, key="key2")
            if last_sent != key2_value:
                fields["key2"] = key2_value

        if fields:
            await self.teyca_client.update_pass_fields(
                user_id=user_id,
                fields=fields,
                rate_limit_max_wait_seconds=self._teyca_rate_limit_max_wait_seconds(),
            )
            for key, value in fields.items():
                await self._set_teyca_key_value(
                    user_id=user_id, key=cast(Literal["key1", "key2"], key), value=str(value)
                )
        if include_key1:
            payload["key1_done"] = True
            await self._bonus_save_progress(idempotency_key=idempotency_key, payload=payload)
        if include_key2:
            merge_payload["key2_done"] = True
            await self._save_progress(outbox_id=merge_claim.id, payload=merge_payload)

        if not merge_payload["merge_logged"]:
            await self._write_merge_log(
                user_id=user_id,
                source_event_type=merge_payload["source_event_type"],
                source_event_id=merge_claim.source_event_id,
                trace_id=merge_claim.trace_id,
            )
            merge_payload["merge_logged"] = True

        await self._bonus_mark_done(idempotency_key=idempotency_key, payload=payload)
        await self._mark_done(outbox_id=merge_claim.id, payload=merge_payload)
        logger.info(
            "external_dispatcher_consent_and_merge_combined_done",
            user_id=user_id,
            merge_outbox_id=merge_claim.id,
            bonus_call_made=bool(bonuses),
            field_call_made=bool(fields),
            bonus_done=payload["bonus_done"],
            key1_done=payload["key1_done"],
            merge_key2_done=merge_payload["key2_done"],
            merge_logged=merge_payload["merge_logged"],
        )

    async def _bonus_reserve(
        self,
        *,
        user_id: int,
        idempotency_key: str,
        payload: dict[str, Any],
    ) -> bool:
        async def operation(session: AsyncSession) -> bool:
            repo = BonusAccrualRepository(session)
            return await repo.reserve(
                user_id=user_id,
                reason=BONUS_REASON_EMAIL_CONSENT,
                idempotency_key=idempotency_key,
                payload=payload,
            )

        return bool(await self._run_in_session(operation))

    async def _bonus_get_payload(self, *, idempotency_key: str) -> dict[str, Any] | None:
        async def operation(session: AsyncSession) -> dict[str, Any] | None:
            repo = BonusAccrualRepository(session)
            current = await repo.get_by_key(idempotency_key=idempotency_key)
            return None if current is None else dict(current.payload or {})

        return await self._run_in_session(operation)

    async def _bonus_save_progress(
        self,
        *,
        idempotency_key: str,
        payload: dict[str, Any],
    ) -> None:
        async def operation(session: AsyncSession) -> None:
            repo = BonusAccrualRepository(session)
            await repo.save_progress(
                idempotency_key=idempotency_key,
                payload=payload,
                status="pending",
                error_text=None,
            )

        await self._run_in_session(operation)

    async def _bonus_mark_done(self, *, idempotency_key: str, payload: dict[str, Any]) -> None:
        async def operation(session: AsyncSession) -> None:
            repo = BonusAccrualRepository(session)
            await repo.mark_done_with_payload(idempotency_key=idempotency_key, payload=payload)

        await self._run_in_session(operation)


def _initial_bonus_payload() -> dict[str, Any]:
    return {"bonus_done": False, "key1_done": False}


def _normalize_bonus_payload(raw_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "bonus_done": bool(raw_payload.get("bonus_done", False)),
        "key1_done": bool(raw_payload.get("key1_done", False)),
    }


def _payload_text(payload: dict[str, Any], *, key: str) -> str | None:
    value = payload.get(key)
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _payload_optional_int(payload: dict[str, Any], *, key: str) -> int | None:
    value = payload.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def _payload_optional_float(payload: dict[str, Any], *, key: str) -> float | None:
    value = payload.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _payload_int_list(payload: dict[str, Any], *, key: str) -> list[int]:
    raw = payload.get(key)
    if not isinstance(raw, list):
        return []
    result: list[int] = []
    for item in raw:
        if isinstance(item, bool):
            continue
        if isinstance(item, int):
            result.append(item)
            continue
        if isinstance(item, str) and item.strip().isdigit():
            result.append(int(item.strip()))
    return result


def _normalize_merge_payload(raw_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "bonus_done": bool(raw_payload.get("bonus_done", False)),
        "key2_done": bool(raw_payload.get("key2_done", False)),
        "merge_logged": bool(raw_payload.get("merge_logged", False)),
        "old_bonus_value": raw_payload.get("old_bonus_value"),
        "merge_key2_value": _payload_text(raw_payload, key="merge_key2_value"),
        "source_event_type": _payload_text(raw_payload, key="source_event_type") or "UPDATE",
    }


def build_external_dispatcher_worker(
    *,
    operations: Sequence[str] | None = None,
    worker_id_prefix: str = "external-dispatcher",
) -> ExternalDispatcherWorker:
    settings = get_settings()
    configured_operations = tuple(dict.fromkeys(operations or DEFAULT_OUTBOX_OPERATIONS))
    return ExternalDispatcherWorker(
        settings=settings,
        session_factory=SessionLocal,
        listmonk_client=ListmonkSDKClient(settings),
        teyca_client=build_teyca_client(settings, session_factory=SessionLocal),
        worker_id=f"{worker_id_prefix}:{uuid4().hex}",
        operations=configured_operations,
    )
