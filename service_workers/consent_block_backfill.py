"""One-time backfill: seed `teyca_block_consent` outbox tasks for the
6362-record unsubscribe backlog from the teyca-sync-i6r outage
(teyca-sync-dd2.1, retroactive gap).

The i6r emergency fix (2026-07-31) stopped `consent_sync` from calling Teyca
synchronously — it started marking `blocked` locally only (`mark_checked`),
*before* dd2.1's outbox-enqueue existed. By the time dd2.1 shipped, the
watermark had already advanced past all 6362 records, so `consent_sync` will
never re-observe them and never enqueue a delivery task for them — they were
resolved locally but Teyca still shows their old `key1` value (commonly
`confirmed`), which is exactly the inconsistency dd2.1 was meant to fix.

This script walks every `listmonk_users` row currently blocked/blocklisted/
blacklisted and enqueues `teyca_block_consent` retroactively. Idempotent via
`enqueue_once` with the same dedupe key the live path uses
(`dedupe_key_for_consent_block`), so running this repeatedly, or overlapping
with a live detection for the same user, never creates a duplicate task.
Unlike `consent_bonus_backfill.py`, this script never calls Teyca directly —
it only seeds the outbox; the existing low-priority dispatcher
(`external-dispatcher-consent-block`) delivers under budget, and
`_send_teyca_key_if_changed` skips the call entirely if Teyca already shows
`blocked` for a given user.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings, get_settings
from app.db.session import SessionLocal
from app.repositories.external_call_outbox import (
    OUTBOX_OP_TEYCA_BLOCK_CONSENT,
    ExternalCallOutboxRepository,
    dedupe_key_for_consent_block,
)
from app.repositories.listmonk_users import ListmonkUsersRepository

logger = structlog.get_logger()

TEYCA_KEY1_BLOCKED = "blocked"
BLOCKED_STATUSES = ("blocked", "blocklisted", "blacklisted")


@dataclass(slots=True, frozen=True)
class ConsentBlockCandidate:
    """One user whose blocked state has no outstanding delivery task yet."""

    user_id: int


@dataclass(slots=True)
class ConsentBlockBackfillSummary:
    """Aggregated counts for one backfill run."""

    candidates: int = 0
    enqueued: int = 0
    already_queued: int = 0


@dataclass(slots=True)
class ConsentBlockBackfill:
    """One-time backfill helper for the 6362-record unsubscribe backlog."""

    settings: Settings
    session_factory: async_sessionmaker[AsyncSession]

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

    async def collect_candidates(self) -> list[ConsentBlockCandidate]:
        """Return every currently blocked/blocklisted/blacklisted user."""

        async def operation(session: AsyncSession) -> list[ConsentBlockCandidate]:
            listmonk_repo = ListmonkUsersRepository(session)
            rows = await listmonk_repo.get_by_statuses(statuses=list(BLOCKED_STATUSES))
            return [ConsentBlockCandidate(user_id=int(row.user_id)) for row in rows]

        return await self._run_in_session(operation)

    async def enqueue(
        self, *, candidates: list[ConsentBlockCandidate]
    ) -> ConsentBlockBackfillSummary:
        """Seed a `teyca_block_consent` task for every candidate.

        `enqueue_once` is a no-op when this user already has a task (from a
        prior backfill run or a live detection) — never creates a duplicate.
        """
        summary = ConsentBlockBackfillSummary(candidates=len(candidates))
        for candidate in candidates:
            created = await self._enqueue_one(user_id=candidate.user_id)
            if created:
                summary.enqueued += 1
                logger.info("consent_block_backfill_enqueued", user_id=candidate.user_id)
            else:
                summary.already_queued += 1
        return summary

    async def _enqueue_one(self, *, user_id: int) -> bool:
        async def operation(session: AsyncSession) -> bool:
            outbox = ExternalCallOutboxRepository(session)
            return await outbox.enqueue_once(
                operation=OUTBOX_OP_TEYCA_BLOCK_CONSENT,
                dedupe_key=dedupe_key_for_consent_block(user_id=user_id),
                user_id=user_id,
                payload={"status": TEYCA_KEY1_BLOCKED},
                trace_id=f"consent-block-backfill:{user_id}",
                source_event_id=f"consent-block-backfill:{user_id}",
                queue_name=None,
            )

        return bool(await self._run_in_session(operation))


def build_consent_block_backfill() -> ConsentBlockBackfill:
    """Build backfill helper from current application settings."""
    settings = get_settings()
    return ConsentBlockBackfill(
        settings=settings,
        session_factory=SessionLocal,
    )
