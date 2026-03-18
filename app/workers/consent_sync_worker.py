"""Periodic worker: sync Listmonk consent and accrue consent bonuses."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog import contextvars as log_contextvars

from app.clients.listmonk import ListmonkSDKClient, SubscriberDelta, SubscriberState
from app.clients.teyca import BonusOperation, TeycaAPIError, TeycaClient
from app.config import Settings, get_settings
from app.db.session import SessionLocal
from app.repositories.bonus_accrual import BonusAccrualRepository
from app.repositories.listmonk_users import (
    DuplicateListmonkSubscriberIdError,
    ListmonkUsersRepository,
)
from app.repositories.sync_state import SyncStateRepository

logger = structlog.get_logger()

BONUS_REASON_EMAIL_CONSENT = "email_consent"
TEYCA_KEY1_BLOCKED = "blocked"
TEYCA_KEY1_CONFIRMED = "confirmed"


def parse_list_ids(raw_list_ids: str) -> list[int]:
    """Parse comma-separated LISTMONK_LIST_IDS."""
    result: list[int] = []
    for chunk in raw_list_ids.split(","):
        stripped = chunk.strip()
        if not stripped:
            continue
        try:
            result.append(int(stripped))
        except ValueError:
            continue
    return result


@dataclass(slots=True)
class ConsentSyncWorker:
    """Runs consent sync loop for pending users."""

    settings: Settings
    session_factory: async_sessionmaker[AsyncSession]
    listmonk_client: ListmonkSDKClient
    teyca_client: TeycaClient

    async def _run_in_session(
        self,
        operation: Callable[[AsyncSession], Awaitable[Any]],
    ) -> Any:
        """Run one short database phase in its own transaction."""
        async with self.session_factory() as session:
            try:
                result = await operation(session)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
        return result

    async def _load_watermark(self, *, list_id: int) -> tuple[datetime | None, int | None]:
        """Load or create the current sync watermark for a list."""

        async def operation(session: AsyncSession) -> tuple[datetime | None, int | None]:
            sync_repo = SyncStateRepository(session)
            state = await sync_repo.get_or_create(source="listmonk_consent", list_id=list_id)
            return state.watermark_updated_at, state.watermark_subscriber_id

        return await self._run_in_session(operation)

    async def _update_watermark(
        self,
        *,
        list_id: int,
        updated_at: datetime | None,
        subscriber_id: int | None,
    ) -> None:
        """Persist the latest successfully reviewed watermark for a list."""

        async def operation(session: AsyncSession) -> None:
            sync_repo = SyncStateRepository(session)
            await sync_repo.get_or_create(source="listmonk_consent", list_id=list_id)
            await sync_repo.update_watermark(
                source="listmonk_consent",
                list_id=list_id,
                updated_at=updated_at,
                subscriber_id=subscriber_id,
            )

        await self._run_in_session(operation)

    async def _get_mapped_pending_user(self, *, subscriber_id: int) -> Any | None:
        """Load the local user mapped to a Listmonk subscriber in a short transaction."""

        async def operation(session: AsyncSession) -> Any | None:
            listmonk_repo = ListmonkUsersRepository(session)
            mapped = await listmonk_repo.get_by_subscriber_id(subscriber_id=subscriber_id)
            if mapped is None:
                return None
            return PendingConsentUser(
                user_id=int(mapped.user_id), subscriber_id=int(mapped.subscriber_id)
            )

        return await self._run_in_session(operation)

    async def _mark_checked(
        self,
        *,
        user_id: int,
        subscriber_id: int,
        pending: bool,
        confirmed: bool,
        status: str | None = None,
        listmonk_repo: ListmonkUsersRepository | None = None,
    ) -> None:
        """Persist consent check result after verifying the current mapping."""
        if listmonk_repo is not None:
            await listmonk_repo.mark_checked(
                user_id=user_id,
                pending=pending,
                confirmed=confirmed,
                status=status,
            )
            return

        async def operation(session: AsyncSession) -> None:
            repo = ListmonkUsersRepository(session)
            current = await repo.get_by_user_id(user_id=user_id)
            if current is None or int(current.subscriber_id) != subscriber_id:
                logger.warning(
                    "consent_sync_mapping_changed_skip_mark_checked",
                    user_id=user_id,
                    subscriber_id=subscriber_id,
                    current_subscriber_id=None if current is None else int(current.subscriber_id),
                )
                return
            await repo.mark_checked(
                user_id=user_id,
                pending=pending,
                confirmed=confirmed,
                status=status,
            )

        await self._run_in_session(operation)

    async def _reserve_operation(
        self,
        *,
        user_id: int,
        reason: str,
        idempotency_key: str,
        payload: dict[str, Any] | None,
        accrual_repo: BonusAccrualRepository | None = None,
    ) -> bool:
        """Reserve an idempotent bonus operation."""
        if accrual_repo is not None:
            return await accrual_repo.reserve(
                user_id=user_id,
                reason=reason,
                idempotency_key=idempotency_key,
                payload=payload,
            )

        async def operation(session: AsyncSession) -> bool:
            repo = BonusAccrualRepository(session)
            return await repo.reserve(
                user_id=user_id,
                reason=reason,
                idempotency_key=idempotency_key,
                payload=payload,
            )

        return bool(await self._run_in_session(operation))

    async def _get_operation_payload(
        self,
        *,
        idempotency_key: str,
        accrual_repo: BonusAccrualRepository | None = None,
    ) -> dict[str, Any] | None:
        """Load saved step-progress payload for an idempotent operation."""
        if accrual_repo is not None:
            current = await accrual_repo.get_by_key(idempotency_key=idempotency_key)
            return None if current is None else dict(current.payload or {})

        async def operation(session: AsyncSession) -> dict[str, Any] | None:
            repo = BonusAccrualRepository(session)
            current = await repo.get_by_key(idempotency_key=idempotency_key)
            return None if current is None else dict(current.payload or {})

        return await self._run_in_session(operation)

    async def _save_progress(
        self,
        *,
        idempotency_key: str,
        payload: dict[str, Any],
        status: str,
        error_text: str | None,
        accrual_repo: BonusAccrualRepository | None = None,
    ) -> None:
        """Persist intermediate consent bonus progress in a short transaction."""
        if accrual_repo is not None:
            await accrual_repo.save_progress(
                idempotency_key=idempotency_key,
                payload=payload,
                status=status,
                error_text=error_text,
            )
            return

        async def operation(session: AsyncSession) -> None:
            repo = BonusAccrualRepository(session)
            await repo.save_progress(
                idempotency_key=idempotency_key,
                payload=payload,
                status=status,
                error_text=error_text,
            )

        await self._run_in_session(operation)

    async def _mark_done(
        self,
        *,
        idempotency_key: str,
        payload: dict[str, Any],
        accrual_repo: BonusAccrualRepository | None = None,
    ) -> None:
        """Persist final completed step-progress state."""
        if accrual_repo is not None:
            await accrual_repo.mark_done_with_payload(
                idempotency_key=idempotency_key,
                payload=payload,
            )
            return

        async def operation(session: AsyncSession) -> None:
            repo = BonusAccrualRepository(session)
            await repo.mark_done_with_payload(
                idempotency_key=idempotency_key,
                payload=payload,
            )

        await self._run_in_session(operation)

    async def _process_pending_user(
        self,
        *,
        pending: Any,
        target_list_ids: list[int],
        listmonk_repo: ListmonkUsersRepository | None = None,
        accrual_repo: BonusAccrualRepository | None = None,
        subscriber_override: SubscriberState | None = None,
        metrics: ConsentSyncMetrics | None = None,
    ) -> None:
        user_id = int(pending.user_id)
        subscriber_id = int(pending.subscriber_id)
        idempotency_key = f"{BONUS_REASON_EMAIL_CONSENT}:{user_id}"
        trace_id = f"consent-sync:{user_id}:{subscriber_id}"
        source_event_id = f"consent-sync:{subscriber_id}"

        log_contextvars.bind_contextvars(
            trace_id=trace_id,
            source_event_id=source_event_id,
            user_id=user_id,
        )
        try:
            subscriber = subscriber_override or await self.listmonk_client.get_subscriber_state(
                subscriber_id=subscriber_id
            )
            if subscriber is None:
                _inc(metrics, "subscriber_not_found")
                await self._mark_checked(
                    user_id=user_id,
                    subscriber_id=subscriber_id,
                    pending=True,
                    confirmed=False,
                    listmonk_repo=listmonk_repo,
                )
                logger.info(
                    "consent_sync_subscriber_not_found",
                    user_id=user_id,
                    subscriber_id=subscriber_id,
                )
                return

            normalized_status = subscriber.status.strip().lower()
            blocked_in_targets = subscriber.has_blocked_for_any(target_list_ids=target_list_ids)
            if normalized_status in {"blocked", "blocklisted", "blacklisted"} or blocked_in_targets:
                try:
                    await self.teyca_client.update_pass_fields(
                        user_id=user_id,
                        fields={"key1": TEYCA_KEY1_BLOCKED},
                    )
                    _inc(metrics, "blocked_done")
                    await self._mark_checked(
                        user_id=user_id,
                        subscriber_id=subscriber_id,
                        pending=False,
                        confirmed=False,
                        status=TEYCA_KEY1_BLOCKED,
                        listmonk_repo=listmonk_repo,
                    )
                    logger.info(
                        "consent_sync_blocked",
                        user_id=user_id,
                        subscriber_id=subscriber_id,
                        status=subscriber.status,
                    )
                except TeycaAPIError as exc:
                    await self._mark_checked(
                        user_id=user_id,
                        subscriber_id=subscriber_id,
                        pending=True,
                        confirmed=False,
                        status=TEYCA_KEY1_BLOCKED,
                        listmonk_repo=listmonk_repo,
                    )
                    _inc(metrics, "teyca_errors")
                    logger.error(
                        "consent_sync_blocked_key1_update_failed",
                        user_id=user_id,
                        subscriber_id=subscriber_id,
                        error=str(exc),
                    )
                return

            confirmed = subscriber.is_confirmed_for_all(target_list_ids=target_list_ids)
            if not confirmed:
                _inc(metrics, "not_confirmed")
                await self._mark_checked(
                    user_id=user_id,
                    subscriber_id=subscriber_id,
                    pending=True,
                    confirmed=False,
                    status="unconfirmed",
                    listmonk_repo=listmonk_repo,
                )
                logger.info(
                    "consent_sync_not_confirmed",
                    user_id=user_id,
                    subscriber_id=subscriber_id,
                    status="unconfirmed",
                )
                return

            reserved = await self._reserve_operation(
                user_id=user_id,
                reason=BONUS_REASON_EMAIL_CONSENT,
                idempotency_key=idempotency_key,
                payload=_initial_consent_payload(
                    subscriber_id=subscriber_id,
                    list_ids=subscriber.list_ids,
                ),
                accrual_repo=accrual_repo,
            )
            if not reserved:
                _inc(metrics, "accrual_resumed")
            saved_payload = await self._get_operation_payload(
                idempotency_key=idempotency_key,
                accrual_repo=accrual_repo,
            )
            if saved_payload is None:
                logger.error(
                    "consent_sync_operation_missing",
                    user_id=user_id,
                    subscriber_id=subscriber_id,
                    idempotency_key=idempotency_key,
                )
                _inc(metrics, "operation_missing")
                await self._mark_checked(
                    user_id=user_id,
                    subscriber_id=subscriber_id,
                    pending=True,
                    confirmed=False,
                    status=subscriber.status,
                    listmonk_repo=listmonk_repo,
                )
                return

            bonus_operation = BonusOperation.one_shot(
                value=self.settings.consent_bonus_amount,
            )
            payload = _normalize_progress_payload(
                raw_payload=saved_payload,
                subscriber_id=subscriber_id,
                list_ids=subscriber.list_ids,
            )

            try:
                if not payload["bonus_done"]:
                    await self.teyca_client.accrue_bonuses(
                        user_id=user_id,
                        bonuses=[bonus_operation],
                    )
                    payload["bonus_done"] = True
                    await self._save_progress(
                        idempotency_key=idempotency_key,
                        payload=payload,
                        status="pending",
                        error_text=None,
                        accrual_repo=accrual_repo,
                    )

                if not payload["key1_done"]:
                    await self.teyca_client.update_pass_fields(
                        user_id=user_id,
                        fields={"key1": TEYCA_KEY1_CONFIRMED},
                    )
                    payload["key1_done"] = True
                    await self._save_progress(
                        idempotency_key=idempotency_key,
                        payload=payload,
                        status="pending",
                        error_text=None,
                        accrual_repo=accrual_repo,
                    )

                await self._mark_done(
                    idempotency_key=idempotency_key,
                    payload=payload,
                    accrual_repo=accrual_repo,
                )
                await self._mark_checked(
                    user_id=user_id,
                    subscriber_id=subscriber_id,
                    pending=False,
                    confirmed=True,
                    status=TEYCA_KEY1_CONFIRMED,
                    listmonk_repo=listmonk_repo,
                )
                _inc(metrics, "confirmed_done")
                logger.info(
                    "consent_sync_confirmed_done",
                    user_id=user_id,
                    subscriber_id=subscriber_id,
                    reserved=reserved,
                    bonus_done=payload["bonus_done"],
                    key1_done=payload["key1_done"],
                )
            except TeycaAPIError as exc:
                await self._save_progress(
                    idempotency_key=idempotency_key,
                    payload=payload,
                    status="failed",
                    error_text=str(exc),
                    accrual_repo=accrual_repo,
                )
                await self._mark_checked(
                    user_id=user_id,
                    subscriber_id=subscriber_id,
                    pending=True,
                    confirmed=False,
                    status=subscriber.status,
                    listmonk_repo=listmonk_repo,
                )
                _inc(metrics, "teyca_errors")
                logger.error(
                    "consent_sync_confirmed_step_failed",
                    user_id=user_id,
                    subscriber_id=subscriber_id,
                    error=str(exc),
                    bonus_done=payload["bonus_done"],
                    key1_done=payload["key1_done"],
                )
        finally:
            log_contextvars.unbind_contextvars("trace_id", "source_event_id", "user_id")

    async def run_once(self) -> int:
        """Process one incremental batch. Returns processed count."""
        target_list_ids = parse_list_ids(self.settings.listmonk_list_ids)
        batch_size = max(1, self.settings.consent_sync_batch_size)
        metrics = ConsentSyncMetrics(batch_size=batch_size)

        if not target_list_ids:
            logger.info("consent_sync_no_target_lists")
            return 0

        processed = 0
        for list_id in target_list_ids:
            watermark_updated_at, watermark_subscriber_id = await self._load_watermark(
                list_id=list_id
            )
            deltas = await self.listmonk_client.get_updated_subscribers(
                list_id=list_id,
                watermark_updated_at=watermark_updated_at,
                watermark_subscriber_id=watermark_subscriber_id,
                limit=batch_size,
            )
            metrics.deltas_fetched += len(deltas)
            if not deltas:
                continue

            last_updated_at: datetime | None = None
            last_subscriber_id: int | None = None
            for delta in deltas:
                last_updated_at = delta.updated_at
                last_subscriber_id = delta.subscriber_id
                try:
                    mapped = await self._get_mapped_pending_user(subscriber_id=delta.subscriber_id)
                except DuplicateListmonkSubscriberIdError as exc:
                    metrics.duplicate_subscriber_mappings += 1
                    logger.error(
                        "consent_sync_duplicate_subscriber_mapping",
                        subscriber_id=delta.subscriber_id,
                        list_id=list_id,
                        user_ids=exc.user_ids,
                    )
                    await self._update_watermark(
                        list_id=list_id,
                        updated_at=last_updated_at,
                        subscriber_id=last_subscriber_id,
                    )
                    continue
                if mapped is None:
                    metrics.unmapped_subscribers += 1
                    logger.info(
                        "consent_sync_subscriber_not_mapped",
                        subscriber_id=delta.subscriber_id,
                        list_id=list_id,
                    )
                    await self._update_watermark(
                        list_id=list_id,
                        updated_at=last_updated_at,
                        subscriber_id=last_subscriber_id,
                    )
                    continue

                processed += 1
                await self._process_pending_user(
                    pending=mapped,
                    target_list_ids=target_list_ids,
                    subscriber_override=_delta_to_state(delta),
                    metrics=metrics,
                )
                await self._update_watermark(
                    list_id=list_id,
                    updated_at=last_updated_at,
                    subscriber_id=last_subscriber_id,
                )

            logger.info(
                "consent_sync_list_processed",
                list_id=list_id,
                deltas=len(deltas),
                watermark_updated_at=last_updated_at.isoformat() if last_updated_at else None,
                watermark_subscriber_id=last_subscriber_id,
            )
        logger.info(
            "consent_sync_metrics",
            processed=processed,
            batch_size=metrics.batch_size,
            consent_bonus_amount=self.settings.consent_bonus_amount,
            deltas_fetched=metrics.deltas_fetched,
            unmapped_subscribers=metrics.unmapped_subscribers,
            duplicate_subscriber_mappings=metrics.duplicate_subscriber_mappings,
            subscriber_not_found=metrics.subscriber_not_found,
            blocked_done=metrics.blocked_done,
            not_confirmed=metrics.not_confirmed,
            confirmed_done=metrics.confirmed_done,
            accrual_resumed=metrics.accrual_resumed,
            operation_missing=metrics.operation_missing,
            teyca_errors=metrics.teyca_errors,
        )
        return processed


def build_consent_sync_worker() -> ConsentSyncWorker:
    """Build worker instance from application settings."""
    settings = get_settings()
    return ConsentSyncWorker(
        settings=settings,
        session_factory=SessionLocal,
        listmonk_client=ListmonkSDKClient(settings),
        teyca_client=TeycaClient(settings),
    )


def _initial_consent_payload(*, subscriber_id: int, list_ids: list[int]) -> dict[str, Any]:
    return {
        "subscriber_id": subscriber_id,
        "list_ids": list_ids,
        "bonus_done": False,
        "key1_done": False,
    }


def _normalize_progress_payload(
    *,
    raw_payload: dict[str, Any] | None,
    subscriber_id: int,
    list_ids: list[int],
) -> dict[str, Any]:
    payload = dict(raw_payload or {})
    payload["subscriber_id"] = int(payload.get("subscriber_id", subscriber_id))
    payload["list_ids"] = payload.get("list_ids", list_ids)
    payload["bonus_done"] = bool(payload.get("bonus_done", False))
    payload["key1_done"] = bool(payload.get("key1_done", False))
    return payload


def _delta_to_state(delta: SubscriberDelta) -> SubscriberState:
    return SubscriberState(
        subscriber_id=delta.subscriber_id,
        status=delta.status,
        list_ids=delta.list_ids,
        list_statuses=delta.list_statuses,
    )


@dataclass(slots=True)
class ConsentSyncMetrics:
    """Aggregated counters for one worker run."""

    batch_size: int
    deltas_fetched: int = 0
    unmapped_subscribers: int = 0
    duplicate_subscriber_mappings: int = 0
    subscriber_not_found: int = 0
    blocked_done: int = 0
    not_confirmed: int = 0
    confirmed_done: int = 0
    accrual_resumed: int = 0
    operation_missing: int = 0
    teyca_errors: int = 0


@dataclass(slots=True, frozen=True)
class PendingConsentUser:
    """Minimal mapped user state needed to process one consent delta."""

    user_id: int
    subscriber_id: int


def _inc(metrics: ConsentSyncMetrics | None, field_name: str) -> None:
    if metrics is None:
        return
    current = getattr(metrics, field_name)
    setattr(metrics, field_name, int(current) + 1)
