"""Repair duplicate listmonk subscriber mappings using authoritative Listmonk user_id."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import httpx
import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.listmonk import ListmonkClientError, ListmonkSDKClient, SubscriberProfile
from app.config import Settings, get_settings
from app.db.session import SessionLocal
from app.repositories.listmonk_user_archive import ListmonkUserArchiveRepository
from app.repositories.listmonk_users import ListmonkUsersRepository

logger = structlog.get_logger()

ARCHIVE_REASON_DUPLICATE_SUBSCRIBER = "duplicate_subscriber_id"


@dataclass(slots=True)
class DuplicateSubscriberRepairMetrics:
    batch_size: int
    scanned: int = 0
    resolved: int = 0
    archived_rows: int = 0
    manual_review: int = 0
    listmonk_errors: int = 0


@dataclass(slots=True)
class ListmonkDuplicateSubscriberWorker:
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

    async def _load_duplicate_subscriber_ids(self, *, limit: int) -> list[int]:
        """Load current duplicate subscriber ids without holding transaction during I/O."""

        async def operation(session: AsyncSession) -> list[int]:
            listmonk_repo = ListmonkUsersRepository(session)
            return await listmonk_repo.get_duplicate_subscriber_ids(limit=limit)

        return await self._run_in_session(operation)

    async def _load_rows_by_subscriber_id(self, *, subscriber_id: int) -> list[Any]:
        """Load current rows for one duplicate subscriber id."""

        async def operation(session: AsyncSession) -> list[Any]:
            listmonk_repo = ListmonkUsersRepository(session)
            return await listmonk_repo.get_rows_by_subscriber_id(subscriber_id=subscriber_id)

        return await self._run_in_session(operation)

    async def _apply_repair(
        self,
        *,
        subscriber_id: int,
        winner_user_id: int,
    ) -> int:
        """Archive losers and remove duplicate mappings in a short transaction."""

        async def operation(session: AsyncSession) -> int:
            listmonk_repo = ListmonkUsersRepository(session)
            archive_repo = ListmonkUserArchiveRepository(session)
            rows = await listmonk_repo.get_rows_by_subscriber_id(subscriber_id=subscriber_id)
            winners = [row for row in rows if int(row.user_id) == winner_user_id]
            if len(winners) != 1:
                logger.warning(
                    "listmonk_duplicate_subscriber_apply_skipped",
                    subscriber_id=subscriber_id,
                    winner_user_id=winner_user_id,
                    current_user_ids=[int(row.user_id) for row in rows],
                )
                return 0
            losers = [row for row in rows if int(row.user_id) != winner_user_id]
            for loser in losers:
                await archive_repo.archive_loser(
                    row=loser,
                    winner_user_id=winner_user_id,
                    winner_subscriber_id=subscriber_id,
                    archive_reason=ARCHIVE_REASON_DUPLICATE_SUBSCRIBER,
                )
            if losers:
                await listmonk_repo.delete_by_user_ids(
                    user_ids=[int(row.user_id) for row in losers],
                )
            return len(losers)

        return int(await self._run_in_session(operation))

    async def run_once(self) -> int:
        batch_size = max(1, self.settings.consent_sync_batch_size)
        metrics = DuplicateSubscriberRepairMetrics(batch_size=batch_size)

        subscriber_ids = await self._load_duplicate_subscriber_ids(limit=batch_size)
        if not subscriber_ids:
            logger.info("listmonk_duplicate_subscriber_no_rows", batch_size=batch_size)
            return 0

        for subscriber_id in subscriber_ids:
            metrics.scanned += 1
            rows = await self._load_rows_by_subscriber_id(subscriber_id=subscriber_id)
            try:
                profile = await self.listmonk_client.get_subscriber_profile(
                    subscriber_id=subscriber_id
                )
            except (ListmonkClientError, httpx.HTTPError) as exc:
                metrics.listmonk_errors += 1
                logger.error(
                    "listmonk_duplicate_subscriber_fetch_failed",
                    subscriber_id=subscriber_id,
                    user_ids=[int(row.user_id) for row in rows],
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                continue

            winner_user_id = _extract_authoritative_user_id(profile)
            if winner_user_id is None:
                metrics.manual_review += 1
                logger.error(
                    "listmonk_duplicate_subscriber_manual_review",
                    subscriber_id=subscriber_id,
                    reason="missing_authoritative_user_id",
                    user_ids=[int(row.user_id) for row in rows],
                    listmonk_email=profile.email if profile is not None else None,
                )
                continue

            winners = [row for row in rows if int(row.user_id) == winner_user_id]
            if len(winners) != 1:
                metrics.manual_review += 1
                logger.error(
                    "listmonk_duplicate_subscriber_manual_review",
                    subscriber_id=subscriber_id,
                    reason="winner_not_unique",
                    winner_user_id=winner_user_id,
                    user_ids=[int(row.user_id) for row in rows],
                    listmonk_email=profile.email if profile is not None else None,
                )
                continue

            winner = winners[0]
            archived_rows = await self._apply_repair(
                subscriber_id=subscriber_id,
                winner_user_id=winner_user_id,
            )
            if archived_rows == 0:
                metrics.manual_review += 1
                logger.error(
                    "listmonk_duplicate_subscriber_manual_review",
                    subscriber_id=subscriber_id,
                    reason="winner_not_unique_after_recheck",
                    winner_user_id=winner_user_id,
                    user_ids=[int(row.user_id) for row in rows],
                    listmonk_email=profile.email if profile is not None else None,
                )
                continue
            metrics.resolved += 1
            metrics.archived_rows += archived_rows
            logger.info(
                "listmonk_duplicate_subscriber_repaired",
                subscriber_id=subscriber_id,
                winner_user_id=winner_user_id,
                loser_user_ids=[
                    int(row.user_id) for row in rows if int(row.user_id) != winner_user_id
                ],
                winner_email=winner.email,
                listmonk_email=profile.email if profile is not None else None,
            )

        logger.info(
            "listmonk_duplicate_subscriber_metrics",
            batch_size=metrics.batch_size,
            scanned=metrics.scanned,
            resolved=metrics.resolved,
            archived_rows=metrics.archived_rows,
            manual_review=metrics.manual_review,
            listmonk_errors=metrics.listmonk_errors,
        )
        return metrics.resolved


def build_listmonk_duplicate_subscriber_worker() -> ListmonkDuplicateSubscriberWorker:
    settings = get_settings()
    return ListmonkDuplicateSubscriberWorker(
        settings=settings,
        session_factory=SessionLocal,
        listmonk_client=ListmonkSDKClient(settings),
    )


def _extract_authoritative_user_id(profile: SubscriberProfile | None) -> int | None:
    if profile is None or not profile.attributes:
        return None
    raw = profile.attributes.get("user_id")
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str) and raw.strip().isdigit():
        return int(raw.strip())
    return None
