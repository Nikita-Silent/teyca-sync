"""One-time cleanup for stale email_repair_log pending rows (teyca-sync-y1c).

email_repair_worker.py (the Listmonk-truth resolver for `pending` rows) was
never scheduled, so conflicts recorded before the y1c policy backfill
shipped were never marked done — even after the underlying duplicate group
got resolved under a *different* email_repair_log row. This never calls
Teyca: a row only qualifies as stale when the conflict it describes no
longer exists in `users`, so there is nothing left to sync.
"""

from __future__ import annotations

from dataclasses import dataclass

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.session import SessionLocal
from app.repositories.email_repair_log import EmailRepairLogRepository

logger = structlog.get_logger()

STALE_REASON = (
    "normalized_email no longer matches both users (teyca-sync-y1c stale-pending cleanup)"
)


@dataclass(slots=True)
class StalePendingCandidate:
    """A pending row whose recorded conflict no longer exists."""

    repair_id: int
    normalized_email: str
    incoming_user_id: int
    existing_user_id: int


@dataclass(slots=True)
class StalePendingCleanupSummary:
    """Aggregated counts for one cleanup run."""

    candidates: int = 0
    marked_stale: int = 0


@dataclass(slots=True)
class StalePendingRepairCleanup:
    """Marks stale `pending` email_repair_log rows without calling Teyca."""

    session_factory: async_sessionmaker[AsyncSession]

    async def collect(self, *, batch_size: int) -> list[StalePendingCandidate]:
        """Find pending rows whose conflict no longer exists, up to batch_size."""
        async with self.session_factory() as session:
            repair_repo = EmailRepairLogRepository(session)
            rows = await repair_repo.get_stale_pending_batch(limit=batch_size)

        return [
            StalePendingCandidate(
                repair_id=int(row.id),
                normalized_email=row.normalized_email,
                incoming_user_id=int(row.incoming_user_id),
                existing_user_id=int(row.existing_user_id),
            )
            for row in rows
        ]

    async def apply(
        self, *, candidates: list[StalePendingCandidate]
    ) -> StalePendingCleanupSummary:
        """Mark every candidate row `stale` in one short transaction."""
        summary = StalePendingCleanupSummary(candidates=len(candidates))
        async with self.session_factory() as session:
            repair_repo = EmailRepairLogRepository(session)
            for candidate in candidates:
                await repair_repo.mark_stale(repair_id=candidate.repair_id, reason=STALE_REASON)
                summary.marked_stale += 1
            await session.commit()

        logger.info(
            "stale_pending_repair_cleanup_apply_completed",
            candidates=summary.candidates,
            marked_stale=summary.marked_stale,
        )
        return summary


def build_stale_pending_repair_cleanup() -> StalePendingRepairCleanup:
    """Build cleanup helper with the default session factory."""
    return StalePendingRepairCleanup(session_factory=SessionLocal)
