"""Integration test: Postgres itself becomes unavailable mid-flight
(teyca-sync-9ib) — one of the three "game day" scenarios called for by the
epic (Listmonk, Teyca, and the database, killed one at a time). Teyca's is
covered by test_teyca_fault_injection.py, Listmonk's by
test_listmonk_fault_injection.py; this covers the database.

Runs its own single-use Postgres container (stopped and restarted mid-test)
rather than the shared module-scoped one in conftest.py, so this test
doesn't take down Postgres out from under the other integration tests in
the same pytest session.
"""

from __future__ import annotations

import os
import subprocess
import time
from collections.abc import AsyncGenerator, Generator
from datetime import UTC, datetime, timedelta

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.repositories.webhook_inbox import WebhookInboxRepository
from app.workers.scheduled_task import ScheduledTask, run_scheduled_task
from tests.integration.conftest import (
    PORT_RANGE_END,
    PORT_RANGE_START,
    REPO_ROOT,
    _candidate_ports,
    _container_name,
    _start_postgres,
)


@pytest.fixture
def killable_postgres_url() -> Generator[tuple[str, str]]:
    """Sync fixture (like conftest.py's `postgres_url`): `alembic upgrade
    head` runs its own `asyncio.run()` internally and must not be called
    from inside a running event loop, so this can't be an async fixture."""
    container_name = _container_name("killable")
    host_port: int | None = None
    for candidate in _candidate_ports(seed=2):
        if _start_postgres(container_name, candidate):
            host_port = candidate
            break
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, check=False)
    if host_port is None:
        raise RuntimeError(
            f"could not start postgres on any candidate port in "
            f"{PORT_RANGE_START}-{PORT_RANGE_END}"
        )

    url = f"postgresql+asyncpg://postgres:test@localhost:{host_port}/test?ssl=disable"
    os.environ["DATABASE_URL"] = url
    config = Config(str(REPO_ROOT / "alembic.ini"))
    command.upgrade(config, "head")
    try:
        yield url, container_name
    finally:
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, check=False)


@pytest.fixture
async def killable_engine(
    killable_postgres_url: tuple[str, str],
) -> AsyncGenerator[tuple[AsyncEngine, str]]:
    url, container_name = killable_postgres_url
    engine = create_async_engine(url, pool_pre_ping=True)
    try:
        yield engine, container_name
    finally:
        await engine.dispose()


def _stop_container(container_name: str) -> None:
    subprocess.run(["docker", "stop", "-t", "1", container_name], capture_output=True, check=True)


def _start_container(container_name: str) -> None:
    subprocess.run(["docker", "start", container_name], capture_output=True, check=True)
    # Give the server a moment to accept connections again before the test
    # continues — no "ready twice" log-marker wait needed here since this is
    # a restart of an already-initialized data directory, not first boot.
    time.sleep(2.0)


@pytest.mark.asyncio
async def test_operation_fails_fast_while_db_is_down_and_recovers_without_data_loss(
    killable_engine: tuple[AsyncEngine, str],
) -> None:
    engine, container_name = killable_engine
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # Sanity: a normal operation succeeds before anything is killed.
    async with session_factory() as session:
        repo = WebhookInboxRepository(session)
        inserted = await repo.enqueue(
            source_event_id="game-day-event-1",
            event_type="CREATE",
            payload={"type": "CREATE", "pass": {"user_id": 1}},
            trace_id="game-day",
        )
        await session.commit()
    assert inserted is True

    _stop_container(container_name)

    started_at = datetime.now(UTC)
    with pytest.raises(Exception):  # noqa: B017 - asyncpg/sqlalchemy raise several distinct types
        async with session_factory() as session:
            repo = WebhookInboxRepository(session)
            await repo.enqueue(
                source_event_id="game-day-event-2",
                event_type="CREATE",
                payload={"type": "CREATE", "pass": {"user_id": 2}},
                trace_id="game-day",
            )
            await session.commit()
    failed_after = datetime.now(UTC) - started_at
    # Fails fast (connection refused), not by hanging until some long
    # default timeout — proves the outage is visible quickly rather than
    # silently swallowing requests for minutes.
    assert failed_after < timedelta(seconds=10)

    _start_container(container_name)

    # Once the database is back, the same operation succeeds and the row
    # that failed to insert during the outage was never partially written —
    # there's exactly the one row from before the outage, plus this retry.
    async with session_factory() as session:
        repo = WebhookInboxRepository(session)
        inserted = await repo.enqueue(
            source_event_id="game-day-event-2",
            event_type="CREATE",
            payload={"type": "CREATE", "pass": {"user_id": 2}},
            trace_id="game-day",
        )
        await session.commit()
    assert inserted is True

    async with engine.connect() as conn:
        count = (
            await conn.execute(text("SELECT count(*) FROM webhook_inbox"))
        ).scalar_one()
    assert count == 2


@pytest.mark.asyncio
async def test_scheduled_task_survives_db_outage_without_crashing_the_process(
    killable_engine: tuple[AsyncEngine, str],
) -> None:
    """The merged worker process (teyca-sync-2g7) must not die just because
    one iteration's DB call fails during an outage — run_scheduled_task
    already proves this against a mocked exception
    (test_exception_is_caught_and_loop_continues); this proves it against a
    real dropped Postgres connection."""
    import asyncio

    engine, container_name = killable_engine
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    _stop_container(container_name)

    shutdown_event = asyncio.Event()
    attempts = 0

    async def run_once() -> int:
        nonlocal attempts
        attempts += 1
        async with session_factory() as session:
            repo = WebhookInboxRepository(session)
            await repo.claim_batch(limit=1, worker_id="game-day-worker")
            await session.commit()
        return 0  # unreachable while the DB is down; kept for typing symmetry

    task = ScheduledTask(name="game-day", run_once=run_once, interval_seconds=0.05)

    async def stop_after_two_attempts() -> None:
        while attempts < 2:
            await asyncio.sleep(0.01)
        shutdown_event.set()

    watchdog = asyncio.create_task(stop_after_two_attempts())
    await asyncio.wait_for(
        run_scheduled_task(task, shutdown_event=shutdown_event), timeout=15
    )
    await watchdog

    assert attempts >= 2
    _start_container(container_name)
