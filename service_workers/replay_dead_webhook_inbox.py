"""One-off tool: replay `webhook_inbox` rows stuck in `dead` (teyca-sync-iil.6).

Since teyca-sync-iil.4/iil.5, a schema mismatch in a webhook payload no longer
drops the event silently (422 that Teyca never retries) — it lands as a
`dead` row instead, with `last_error` describing what pydantic rejected. Once
the schema is fixed (e.g. teyca-sync-iil.1's `tags` null-hole fix), those rows
are still `dead`: nothing re-queues them automatically. This tool does that.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.session import SessionLocal
from app.repositories.webhook_inbox import DeadInboxRow, WebhookInboxRepository

logger = structlog.get_logger()


@dataclass(slots=True)
class ReplaySummary:
    """Aggregated counts for one replay run."""

    candidates: int = 0
    replayed: int = 0


@dataclass(slots=True)
class DeadWebhookInboxReplay:
    """Lists and requeues `dead` webhook_inbox rows."""

    session_factory: async_sessionmaker[AsyncSession]

    async def collect(
        self,
        *,
        batch_size: int,
        since: datetime | None = None,
        event_type: str | None = None,
    ) -> list[DeadInboxRow]:
        """Find dead rows matching the filters, up to batch_size."""
        async with self.session_factory() as session:
            repo = WebhookInboxRepository(session)
            return await repo.get_dead_batch(
                limit=batch_size, since=since, event_type=event_type
            )

    async def apply(self, *, rows: list[DeadInboxRow]) -> ReplaySummary:
        """Reset every row back to pending in one short transaction."""
        summary = ReplaySummary(candidates=len(rows))
        if not rows:
            return summary
        async with self.session_factory() as session:
            repo = WebhookInboxRepository(session)
            summary.replayed = await repo.replay_dead(inbox_ids=[row.id for row in rows])
            await session.commit()

        logger.info(
            "replay_dead_webhook_inbox_apply_completed",
            candidates=summary.candidates,
            replayed=summary.replayed,
        )
        return summary


def build_dead_webhook_inbox_replay() -> DeadWebhookInboxReplay:
    """Build replay helper with the default session factory."""
    return DeadWebhookInboxReplay(session_factory=SessionLocal)
