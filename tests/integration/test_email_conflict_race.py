"""Integration test: two user_ids racing on the same email (teyca-sync-eh8).

Runs against a real Postgres with the actual migration chain applied —
the unique index and the constraint-violation catch only mean something
against real DB uniqueness enforcement, not mocks. Manages the container
directly via the `docker` CLI rather than testcontainers' PostgresContainer:
testcontainers' Ryuk reaper cannot reach published ports under rootless
Docker in this environment, while a plain `docker run -p` mapping works —
as long as the published port isn't in the kernel's ephemeral range
(/proc/sys/net/ipv4/ip_local_port_range, 32768-60999 here), which the
rootless port forwarder cannot reliably serve.
"""

from __future__ import annotations

import os
import subprocess
import time
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.consumers.email_conflict import is_email_unique_violation, resolve_users_email_conflict
from app.repositories.external_call_outbox import ExternalCallOutboxRepository
from app.repositories.users import UsersRepository

REPO_ROOT = Path(__file__).resolve().parents[2]
# Per-process container name so two concurrent pytest runs (e.g.
# `make coverage` overlapping another run) don't remove each other's
# container.
CONTAINER_NAME = f"teyca-sync-eh8-race-test-pg-{os.getpid()}"
PORT_RANGE_START = 15000
PORT_RANGE_END = 32000
PORT_ATTEMPTS = 12


def _candidate_ports() -> list[int]:
    """Ports to try, spread by PID so concurrent runs rarely start on the same one."""
    span = PORT_RANGE_END - PORT_RANGE_START
    first = (os.getpid() * 97) % span
    return [PORT_RANGE_START + (first + step * 137) % span for step in range(PORT_ATTEMPTS)]


READY_LOG_MARKER = "database system is ready to accept connections"


def _server_truly_ready(*, timeout_seconds: float = 30.0) -> bool:
    """Wait for Postgres to be ready for real, external connections.

    The official image logs "database system is ready to accept
    connections" once after initdb, then restarts internally, then logs
    it again once the server that actually serves external clients is
    up. `pg_isready`/a bare TCP connect can succeed during that first,
    short-lived window — the connection then resets mid-handshake right
    after. Waiting for the marker twice avoids that race.
    """
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        logs = subprocess.run(
            ["docker", "logs", CONTAINER_NAME], capture_output=True, text=True
        )
        if (logs.stdout + logs.stderr).count(READY_LOG_MARKER) >= 2:
            return True
        time.sleep(0.25)
    return False


def _start_postgres(host_port: int) -> bool:
    """Start the container on `host_port`; True if it came up and is reachable."""
    started = subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            CONTAINER_NAME,
            "-e",
            "POSTGRES_PASSWORD=test",
            "-e",
            "POSTGRES_DB=test",
            "-p",
            f"{host_port}:5432",
            "postgres:17-alpine",
        ],
        capture_output=True,
    )
    if started.returncode != 0:
        return False

    return _server_truly_ready()


@pytest.fixture(scope="module")
def postgres_url() -> Generator[str]:
    host_port: int | None = None
    for candidate in _candidate_ports():
        if _start_postgres(candidate):
            host_port = candidate
            break
        subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True, check=False)
    if host_port is None:
        raise RuntimeError(
            f"could not start postgres on any of {PORT_ATTEMPTS} candidate ports "
            f"in {PORT_RANGE_START}-{PORT_RANGE_END}"
        )

    try:
        # ssl=disable: asyncpg's SSLRequest probe gets reset by this
        # sandbox's docker networking, so skip it — this is a local,
        # unencrypted-by-design test container anyway.
        url = f"postgresql+asyncpg://postgres:test@localhost:{host_port}/test?ssl=disable"
        os.environ["DATABASE_URL"] = url
        config = Config(str(REPO_ROOT / "alembic.ini"))
        command.upgrade(config, "head")
        yield url
    finally:
        subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True, check=False)


@pytest.fixture
async def engine(postgres_url: str) -> AsyncGenerator[AsyncEngine]:
    engine = create_async_engine(postgres_url)
    yield engine
    await engine.dispose()


@pytest.mark.asyncio
async def test_two_user_ids_racing_on_same_email_resolved_automatically(
    engine: AsyncEngine,
) -> None:
    """User 1002 has more recent activity than 1001 (Р5/Р6 policy), so it
    wins the race even though 1001 committed the email first: the real
    uq_users_email_lower_trim violation on 1002's write is caught and
    resolved by swapping the email to the more-recently-active user."""
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as session:
        users_repo = UsersRepository(session)
        await users_repo.upsert(
            user_id=1001, profile={"phone": "+7900", "date_last": "2020-01-01"}
        )
        await users_repo.upsert(
            user_id=1002, profile={"phone": "+7901", "date_last": "2026-01-01"}
        )
        await session.commit()

    # 1001 claims the email first and commits.
    async with session_factory() as session:
        users_repo = UsersRepository(session)
        await users_repo.upsert(
            user_id=1001,
            profile={"email": "race@example.com", "phone": "+7900", "date_last": "2020-01-01"},
        )
        await session.commit()

    # 1002 races for the same email against the real unique index, going
    # through the same lock_user + begin_nested(savepoint) path the
    # consumers use (teyca-sync-x1g.1) rather than a bare upsert.
    profile_1002 = {
        "email": "race@example.com",
        "phone": "+7901",
        "date_last": "2026-01-01",
    }
    won: bool | None = None
    async with session_factory() as session:
        users_repo = UsersRepository(session)
        await users_repo.lock_user(user_id=1002, wait=True)
        try:
            async with session.begin_nested():
                await users_repo.upsert(user_id=1002, profile=profile_1002)
        except IntegrityError as exc:
            assert is_email_unique_violation(exc)
            won = await resolve_users_email_conflict(
                session=session,
                user_id=1002,
                profile=profile_1002,
                source_event_type="UPDATE",
                source_event_id="race-test",
                trace_id="race-test",
            )
            await session.commit()

    assert won is True

    async with session_factory() as session:
        users_repo = UsersRepository(session)
        loser = await users_repo.get_by_user_id(user_id=1001)
        winner = await users_repo.get_by_user_id(user_id=1002)
        assert loser is not None
        assert winner is not None
        assert loser.email is None
        assert winner.email == "race@example.com"

        outbox_repo = ExternalCallOutboxRepository(session)
        claims = await outbox_repo.claim_batch(
            operations=["teyca_email_repair_sync"], limit=10, worker_id="race-test-worker"
        )
        await session.commit()

    assert len(claims) == 1
    assert claims[0].user_id == 1001
    assert claims[0].payload["winner_user_id"] == 1002
    assert claims[0].payload["mark_bad_email"] is True


@pytest.mark.asyncio
async def test_advisory_lock_survives_conflict_resolution(engine: AsyncEngine) -> None:
    """teyca-sync-x1g.1: lock_user's pg_advisory_xact_lock is
    transaction-scoped. A plain session.rollback() inside conflict
    resolution would silently drop it; begin_nested() (SAVEPOINT) must
    not. Proven here by trying to take the same advisory lock from a
    second, independent connection while the first transaction — now
    past conflict resolution — is still open and uncommitted."""
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    lock_key = 2002

    async with session_factory() as seed_session:
        users_repo = UsersRepository(seed_session)
        await users_repo.upsert(
            user_id=2001, profile={"email": "lock-race@example.com", "phone": "+7900"}
        )
        await users_repo.upsert(user_id=lock_key, profile={"phone": "+7901"})
        await seed_session.commit()

    holder_session = session_factory()
    try:
        users_repo = UsersRepository(holder_session)
        await users_repo.lock_user(user_id=lock_key, wait=True)
        profile = {"email": "lock-race@example.com", "phone": "+7901"}
        try:
            async with holder_session.begin_nested():
                await users_repo.upsert(user_id=lock_key, profile=profile)
        except IntegrityError as exc:
            assert is_email_unique_violation(exc)
            await resolve_users_email_conflict(
                session=holder_session,
                user_id=lock_key,
                profile=profile,
                source_event_type="UPDATE",
                source_event_id="lock-test",
                trace_id="lock-test",
            )

        # Conflict resolved, transaction still open/uncommitted: the lock
        # must still be held. A second, independent connection trying the
        # same advisory lock key must be refused.
        async with engine.connect() as other_conn:
            still_held = not (
                await other_conn.execute(
                    text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": lock_key}
                )
            ).scalar_one()
            assert still_held is True

        await holder_session.commit()
    finally:
        await holder_session.close()

    # After commit, the lock is released — a fresh connection can take it.
    async with engine.connect() as other_conn:
        now_free = (
            await other_conn.execute(
                text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": lock_key}
            )
        ).scalar_one()
        assert now_free is True
