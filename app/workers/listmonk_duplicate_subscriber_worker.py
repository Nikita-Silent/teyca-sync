"""Repair duplicate listmonk subscriber mappings using authoritative Listmonk user_id."""

from __future__ import annotations

from dataclasses import dataclass

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

    async def run_once(self) -> int:
        batch_size = max(1, self.settings.consent_sync_batch_size)
        metrics = DuplicateSubscriberRepairMetrics(batch_size=batch_size)

        async with self.session_factory() as session:
            listmonk_repo = ListmonkUsersRepository(session)
            archive_repo = ListmonkUserArchiveRepository(session)

            subscriber_ids = await listmonk_repo.get_duplicate_subscriber_ids(limit=batch_size)
            if not subscriber_ids:
                logger.info("listmonk_duplicate_subscriber_no_rows", batch_size=batch_size)
                return 0

            for subscriber_id in subscriber_ids:
                metrics.scanned += 1
                rows = await listmonk_repo.get_rows_by_subscriber_id(subscriber_id=subscriber_id)
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
                losers = [row for row in rows if int(row.user_id) != winner_user_id]
                for loser in losers:
                    await archive_repo.archive_loser(
                        row=loser,
                        winner_user_id=winner_user_id,
                        winner_subscriber_id=subscriber_id,
                        archive_reason=ARCHIVE_REASON_DUPLICATE_SUBSCRIBER,
                    )
                await listmonk_repo.delete_by_user_ids(
                    user_ids=[int(row.user_id) for row in losers],
                )
                metrics.resolved += 1
                metrics.archived_rows += len(losers)
                logger.info(
                    "listmonk_duplicate_subscriber_repaired",
                    subscriber_id=subscriber_id,
                    winner_user_id=winner_user_id,
                    loser_user_ids=[int(row.user_id) for row in losers],
                    winner_email=winner.email,
                    listmonk_email=profile.email if profile is not None else None,
                )

            await session.commit()

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
