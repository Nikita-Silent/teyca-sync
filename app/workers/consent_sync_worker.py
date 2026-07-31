"""Periodic worker: track Listmonk unsubscribes/blocks for local bookkeeping.

Bonus accrual lives in `app.workers.external_dispatcher_worker` (teyca-sync-4ue,
Р4а): only a successful CRM-triggered `listmonk_upsert` may pay the consent
bonus. This worker never reads or grants bonuses and never calls Teyca — it
only mirrors Listmonk unsubscribe/block state locally so `consent_pending`
reflects "needs a Listmonk recheck", nothing more (Р12).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog import contextvars as log_contextvars

from app.clients.listmonk import ListmonkSDKClient, SubscriberDelta, SubscriberState
from app.config import Settings, get_settings
from app.db.session import SessionLocal
from app.repositories.listmonk_users import (
    DuplicateListmonkSubscriberIdError,
    ListmonkUsersRepository,
)
from app.repositories.sync_state import SyncStateRepository

logger = structlog.get_logger()

TEYCA_KEY1_BLOCKED = "blocked"


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
    """Runs the consent sync loop: unsubscribe/block tracking only."""

    settings: Settings
    session_factory: async_sessionmaker[AsyncSession]
    listmonk_client: ListmonkSDKClient

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
        status: str | None = None,
        listmonk_repo: ListmonkUsersRepository | None = None,
    ) -> None:
        """Persist consent check result after verifying the current mapping."""
        if listmonk_repo is not None:
            await listmonk_repo.mark_checked(
                user_id=user_id,
                pending=pending,
                confirmed=False,
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
                confirmed=False,
                status=status,
            )

        await self._run_in_session(operation)

    async def _process_pending_user(
        self,
        *,
        pending: Any,
        target_list_ids: list[int],
        listmonk_repo: ListmonkUsersRepository | None = None,
        subscriber_override: SubscriberState | None = None,
        metrics: ConsentSyncMetrics | None = None,
    ) -> bool:
        """Track one pending user's unsubscribe/block state.

        Returns True when it is safe to advance the watermark.
        """
        user_id = int(pending.user_id)
        subscriber_id = int(pending.subscriber_id)
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
                    listmonk_repo=listmonk_repo,
                )
                logger.info(
                    "consent_sync_subscriber_not_found",
                    user_id=user_id,
                    subscriber_id=subscriber_id,
                )
                return True

            normalized_status = subscriber.status.strip().lower()
            blocked_in_targets = subscriber.has_blocked_for_any(target_list_ids=target_list_ids)
            if normalized_status in {"blocked", "blocklisted", "blacklisted"} or blocked_in_targets:
                # Р11 (закрыто 2026-07-30): отписки в Teyca не отправляем синхронно —
                # синхронный вызов сюда выжигал суточный лимит (авария, teyca-sync-i6r).
                _inc(metrics, "blocked_done")
                await self._mark_checked(
                    user_id=user_id,
                    subscriber_id=subscriber_id,
                    pending=False,
                    status=TEYCA_KEY1_BLOCKED,
                    listmonk_repo=listmonk_repo,
                )
                logger.info(
                    "consent_sync_blocked",
                    user_id=user_id,
                    subscriber_id=subscriber_id,
                    status=subscriber.status,
                )
                return True

            _inc(metrics, "not_blocked")
            await self._mark_checked(
                user_id=user_id,
                subscriber_id=subscriber_id,
                pending=False,
                status=subscriber.status,
                listmonk_repo=listmonk_repo,
            )
            return True
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

            last_success_updated_at: datetime | None = watermark_updated_at
            last_success_subscriber_id: int | None = watermark_subscriber_id
            stopped_early = False
            for delta in deltas:
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
                    last_success_updated_at = delta.updated_at
                    last_success_subscriber_id = delta.subscriber_id
                    continue
                if mapped is None:
                    metrics.unmapped_subscribers += 1
                    logger.info(
                        "consent_sync_subscriber_not_mapped",
                        subscriber_id=delta.subscriber_id,
                        list_id=list_id,
                    )
                    last_success_updated_at = delta.updated_at
                    last_success_subscriber_id = delta.subscriber_id
                    continue

                processed += 1
                success = await self._process_pending_user(
                    pending=mapped,
                    target_list_ids=target_list_ids,
                    subscriber_override=_delta_to_state(delta),
                    metrics=metrics,
                )
                if success:
                    last_success_updated_at = delta.updated_at
                    last_success_subscriber_id = delta.subscriber_id
                else:
                    stopped_early = True
                    logger.warning(
                        "consent_sync_stopping_on_failure",
                        list_id=list_id,
                        subscriber_id=delta.subscriber_id,
                    )
                    break

            if last_success_subscriber_id != watermark_subscriber_id:
                await self._update_watermark(
                    list_id=list_id,
                    updated_at=last_success_updated_at,
                    subscriber_id=last_success_subscriber_id,
                )

            logger.info(
                "consent_sync_list_processed",
                list_id=list_id,
                deltas=len(deltas),
                stopped_early=stopped_early,
                watermark_updated_at=last_success_updated_at.isoformat()
                if last_success_updated_at
                else None,
                watermark_subscriber_id=last_success_subscriber_id,
            )
        logger.info(
            "consent_sync_metrics",
            processed=processed,
            batch_size=metrics.batch_size,
            deltas_fetched=metrics.deltas_fetched,
            unmapped_subscribers=metrics.unmapped_subscribers,
            duplicate_subscriber_mappings=metrics.duplicate_subscriber_mappings,
            subscriber_not_found=metrics.subscriber_not_found,
            blocked_done=metrics.blocked_done,
            not_blocked=metrics.not_blocked,
        )
        return processed


def build_consent_sync_worker() -> ConsentSyncWorker:
    """Build worker instance from application settings."""
    settings = get_settings()
    return ConsentSyncWorker(
        settings=settings,
        session_factory=SessionLocal,
        listmonk_client=ListmonkSDKClient(settings),
    )


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
    not_blocked: int = 0


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
