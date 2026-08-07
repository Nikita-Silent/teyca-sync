"""Integration test: webhook POST -> Postgres inbox -> worker dispatch (teyca-sync-8ib).

Runs against a real Postgres, like test_worker_restart_mid_processing.py, so the
inbox's SKIP LOCKED claim and crash-recovery reaper are exercised against real
row-level locking instead of a mocked session.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from app.repositories.webhook_inbox import (
    INBOX_STATUS_DONE,
    INBOX_STATUS_PENDING,
    WebhookInboxRepository,
)

AUTH_TOKEN = "36545925e92437d467ec8bef30b07bb2"


async def _backdate_lock(engine: AsyncEngine, *, inbox_id: int, seconds_ago: float) -> None:
    stale_locked_at = datetime.now(UTC) - timedelta(seconds=seconds_ago)
    async with engine.connect() as conn:
        await conn.execute(
            text("UPDATE webhook_inbox SET locked_at = :locked_at WHERE id = :id"),
            {"locked_at": stale_locked_at, "id": inbox_id},
        )
        await conn.commit()


@pytest.mark.asyncio
async def test_webhook_post_persists_event_even_when_downstream_processing_fails(
    postgres_url: str,
    engine: AsyncEngine,
) -> None:
    """Regression test for the bug this migration closes: previously an
    unavailable RabbitMQ broker meant `POST /webhook` raised past the handler
    (webhook.py:149) and the event was never stored anywhere. Now the only
    write is to Postgres itself, so the event survives independently of any
    downstream worker/dispatcher outage."""
    import os

    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
    os.environ["WEBHOOK_AUTH_ENABLED"] = "true"
    os.environ["DATABASE_URL"] = postgres_url

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    with (
        patch("app.db.session.SessionLocal", session_factory),
        patch("app.api.webhook.SessionLocal", session_factory),
    ):
        from app.main import app

        payload = {"type": "CREATE", "pass": {"user_id": 424242}}
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            resp = await ac.post(
                "/webhook",
                json=payload,
                headers={"Authorization": AUTH_TOKEN, "X-Event-Id": "evt-424242"},
            )
    assert resp.status_code == 200

    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT event_type, status, payload FROM webhook_inbox "
                    "WHERE source_event_id = :sid"
                ),
                {"sid": "evt-424242"},
            )
        ).one()
    assert row.event_type == "CREATE"
    assert row.status == INBOX_STATUS_PENDING
    assert row.payload["pass"]["user_id"] == 424242


@pytest.mark.asyncio
async def test_redelivered_webhook_is_deduplicated_in_inbox(
    engine: AsyncEngine,
) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        repo = WebhookInboxRepository(session)
        first = await repo.enqueue(
            source_event_id="dup-event",
            event_type="UPDATE",
            payload={"type": "UPDATE"},
            trace_id="trace-1",
        )
        await session.commit()
    async with session_factory() as session:
        repo = WebhookInboxRepository(session)
        second = await repo.enqueue(
            source_event_id="dup-event",
            event_type="UPDATE",
            payload={"type": "UPDATE"},
            trace_id="trace-2",
        )
        await session.commit()

    assert first is True
    assert second is False

    async with engine.connect() as conn:
        count = (
            await conn.execute(
                text("SELECT count(*) FROM webhook_inbox WHERE source_event_id = :sid"),
                {"sid": "dup-event"},
            )
        ).scalar_one()
    assert count == 1


@pytest.mark.asyncio
async def test_crashed_claim_is_recovered_exactly_once_by_a_restarted_worker(
    engine: AsyncEngine,
) -> None:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # Other tests sharing this module-scoped Postgres may have left pending
    # rows behind; clear the table so claim_batch(limit=1) below deterministically
    # picks up this test's own row.
    async with engine.connect() as conn:
        await conn.execute(text("DELETE FROM webhook_inbox"))
        await conn.commit()

    async with session_factory() as seed_session:
        repo = WebhookInboxRepository(seed_session)
        await repo.enqueue(
            source_event_id="restart-test-event",
            event_type="CREATE",
            payload={"type": "CREATE", "pass": {"user_id": 9002}},
            trace_id="restart-test",
        )
        await seed_session.commit()

    # Worker A claims it, "crashes" before mark_done/mark_retry.
    session_a = session_factory()
    repo_a = WebhookInboxRepository(session_a)
    claims_a = await repo_a.claim_batch(limit=1, worker_id="worker-A")
    assert len(claims_a) == 1
    inbox_id = claims_a[0].id

    # Concurrent worker B must not be able to claim the same locked row.
    async with session_factory() as session_b:
        repo_b = WebhookInboxRepository(session_b)
        claims_b = await repo_b.claim_batch(limit=1, worker_id="worker-B")
        assert claims_b == []
        await session_b.commit()

    await session_a.commit()
    await session_a.close()

    async with engine.connect() as conn:
        status = (
            await conn.execute(
                text("SELECT status FROM webhook_inbox WHERE id = :id"), {"id": inbox_id}
            )
        ).scalar_one()
        assert status == "processing"

    await _backdate_lock(engine, inbox_id=inbox_id, seconds_ago=600.0)

    async with session_factory() as reaper_session:
        reaper_repo = WebhookInboxRepository(reaper_session)
        released = await reaper_repo.release_stale_processing_claims(stale_after_seconds=300.0)
        assert released == 1
        await reaper_session.commit()

    async with session_factory() as session_c:
        repo_c = WebhookInboxRepository(session_c)
        claims_c = await repo_c.claim_batch(limit=1, worker_id="worker-C")
        assert len(claims_c) == 1
        assert claims_c[0].id == inbox_id
        await repo_c.mark_done(inbox_id=inbox_id)
        await session_c.commit()

    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT status, locked_by FROM webhook_inbox WHERE source_event_id = :sid"
                ),
                {"sid": "restart-test-event"},
            )
        ).all()
    assert len(rows) == 1
    assert rows[0].status == INBOX_STATUS_DONE
    assert rows[0].locked_by is None
