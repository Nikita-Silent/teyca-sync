"""Integration test: two user_ids racing on the same email (teyca-sync-eh8).

Runs against a real Postgres with the actual migration chain applied —
the unique index and the constraint-violation catch only mean something
against real DB uniqueness enforcement, not mocks. Container lifecycle is
shared via tests/integration/conftest.py's postgres_url/engine fixtures.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from app.consumers.email_conflict import is_email_unique_violation, resolve_users_email_conflict
from app.repositories.external_call_outbox import ExternalCallOutboxRepository
from app.repositories.users import UsersRepository


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
