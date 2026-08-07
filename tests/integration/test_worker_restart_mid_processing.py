"""Integration test: a worker crashing mid-processing must not lose or
duplicate an outbox job when a fresh worker takes over (teyca-sync-8kh).

Runs against a real Postgres so the two properties under test — SKIP LOCKED
actually preventing two workers from claiming the same row concurrently, and
the reaper's UPDATE actually making a crashed claim visible to claim_batch
again — depend on real row-level locking semantics that an AsyncMock session
can't exercise.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from app.repositories.external_call_outbox import (
    OUTBOX_OP_LISTMONK_UPSERT,
    OUTBOX_STATUS_DONE,
    OUTBOX_STATUS_PENDING,
    ExternalCallOutboxRepository,
)


async def _backdate_lock(engine: AsyncEngine, *, outbox_id: int, seconds_ago: float) -> None:
    """Simulate time passing since a worker claimed the row and then died."""
    stale_locked_at = datetime.now(UTC) - timedelta(seconds=seconds_ago)
    async with engine.connect() as conn:
        await conn.execute(
            text("UPDATE external_call_outbox SET locked_at = :locked_at WHERE id = :id"),
            {"locked_at": stale_locked_at, "id": outbox_id},
        )
        await conn.commit()


@pytest.mark.asyncio
async def test_crashed_claim_is_recovered_exactly_once_by_a_restarted_worker(
    engine: AsyncEngine,
) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as seed_session:
        repo = ExternalCallOutboxRepository(seed_session)
        await repo.enqueue_latest(
            operation=OUTBOX_OP_LISTMONK_UPSERT,
            dedupe_key="restart-test:9001",
            user_id=9001,
            payload={"email": "restart@example.com"},
            trace_id="restart-test",
            source_event_id="restart-test-event",
            queue_name="queue-update",
        )
        await seed_session.commit()

    # Worker A claims it (this is the transaction a crash would abandon
    # between commit and the follow-up mark_done/mark_retry).
    session_a = session_factory()
    repo_a = ExternalCallOutboxRepository(session_a)
    claims_a = await repo_a.claim_batch(
        operations=[OUTBOX_OP_LISTMONK_UPSERT], limit=1, worker_id="worker-A"
    )
    assert len(claims_a) == 1
    outbox_id = claims_a[0].id

    # While A's claim is still open (row-locked), a concurrent worker B must
    # not also be able to claim it — SKIP LOCKED, not a race.
    async with session_factory() as session_b:
        repo_b = ExternalCallOutboxRepository(session_b)
        claims_b = await repo_b.claim_batch(
            operations=[OUTBOX_OP_LISTMONK_UPSERT], limit=1, worker_id="worker-B"
        )
        assert claims_b == []
        await session_b.commit()

    # A "commits" the claim transaction (status=processing persisted) but the
    # process then crashes before calling mark_done/mark_retry — exactly the
    # scenario a container restart mid-processing produces.
    await session_a.commit()
    await session_a.close()

    async with session_factory() as check_session:
        status = (
            await check_session.execute(
                text("SELECT status FROM external_call_outbox WHERE id = :id"),
                {"id": outbox_id},
            )
        ).scalar_one()
        assert status == "processing"

    # Time passes past the stale threshold with no reaper run yet: a fresh
    # worker must still not be able to claim it (not stale yet by the
    # reaper's own bookkeeping) until the reaper actually runs.
    await _backdate_lock(engine, outbox_id=outbox_id, seconds_ago=600.0)
    async with session_factory() as premature_session:
        premature_repo = ExternalCallOutboxRepository(premature_session)
        released = await premature_repo.release_stale_processing_claims(stale_after_seconds=3600.0)
        assert released == 0
        await premature_session.commit()

    # The reaper (with a realistic threshold) reclaims the crashed row.
    async with session_factory() as reaper_session:
        reaper_repo = ExternalCallOutboxRepository(reaper_session)
        released = await reaper_repo.release_stale_processing_claims(stale_after_seconds=300.0)
        assert released == 1
        await reaper_session.commit()

    async with session_factory() as check_session:
        row = (
            await check_session.execute(
                text(
                    "SELECT status, locked_at, locked_by FROM external_call_outbox WHERE id = :id"
                ),
                {"id": outbox_id},
            )
        ).one()
        assert row.status == OUTBOX_STATUS_PENDING
        assert row.locked_at is None
        assert row.locked_by is None

    # A restarted worker (worker-C) claims and completes the recovered row.
    async with session_factory() as session_c:
        repo_c = ExternalCallOutboxRepository(session_c)
        claims_c = await repo_c.claim_batch(
            operations=[OUTBOX_OP_LISTMONK_UPSERT], limit=1, worker_id="worker-C"
        )
        assert len(claims_c) == 1
        assert claims_c[0].id == outbox_id
        assert claims_c[0].payload == {"email": "restart@example.com"}
        await repo_c.mark_done(outbox_id=outbox_id, payload={"synced": True})
        await session_c.commit()

    # Exactly one row for this dedupe key, in a terminal done state — no
    # duplicate processing and no data loss from the simulated crash.
    async with session_factory() as final_session:
        rows = (
            await final_session.execute(
                text(
                    "SELECT status, payload, locked_by FROM external_call_outbox "
                    "WHERE dedupe_key = :dedupe_key"
                ),
                {"dedupe_key": "restart-test:9001"},
            )
        ).all()
        assert len(rows) == 1
        assert rows[0].status == OUTBOX_STATUS_DONE
        assert rows[0].locked_by is None
