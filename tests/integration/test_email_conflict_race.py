"""Integration test: two user_ids racing on the same email (teyca-sync-eh8).

Runs against a real Postgres with the actual migration chain applied —
the unique index and the constraint-violation catch only mean something
against real DB uniqueness enforcement, not mocks. Manages the container
directly via the `docker` CLI rather than testcontainers' PostgresContainer:
testcontainers' Ryuk reaper cannot reach published ports under rootless
Docker in this environment, while a plain `docker run -p` mapping works.
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
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.consumers.email_conflict import is_email_unique_violation, resolve_users_email_conflict
from app.repositories.external_call_outbox import ExternalCallOutboxRepository
from app.repositories.users import UsersRepository

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTAINER_NAME = "teyca-sync-eh8-race-test-pg"
HOST_PORT = 15532


@pytest.fixture(scope="module")
def postgres_url() -> Generator[str]:
    subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True, check=False)
    subprocess.run(
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
            f"{HOST_PORT}:5432",
            "postgres:17-alpine",
        ],
        check=True,
        capture_output=True,
    )
    try:
        for _ in range(60):
            ready = subprocess.run(
                ["docker", "exec", CONTAINER_NAME, "pg_isready", "-U", "postgres"],
                capture_output=True,
            )
            if ready.returncode == 0:
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("postgres container never became ready")

        url = f"postgresql+asyncpg://postgres:test@localhost:{HOST_PORT}/test"
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

    # 1002 races for the same email against the real unique index.
    profile_1002 = {
        "email": "race@example.com",
        "phone": "+7901",
        "date_last": "2026-01-01",
    }
    won: bool | None = None
    async with session_factory() as session:
        users_repo = UsersRepository(session)
        try:
            await users_repo.upsert(user_id=1002, profile=profile_1002)
            await session.commit()
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
