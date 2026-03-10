from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.db.models import MergeLog
from app.repositories.bonus_accrual import BonusAccrualRepository
from app.repositories.listmonk_users import ListmonkUsersRepository
from app.repositories.merge_log import MergeLogRepository
from app.repositories.sync_state import SyncStateRepository
from app.repositories.users import UsersRepository


@pytest.mark.asyncio
async def test_users_repository_paths() -> None:
    session = AsyncMock()
    repo = UsersRepository(session)

    await repo.lock_user(user_id=1)
    session.execute.assert_awaited()

    session.execute.reset_mock()
    session.execute.return_value = SimpleNamespace(scalar_one_or_none=lambda: SimpleNamespace(user_id=1))
    user = await repo.get_by_user_id(user_id=1)
    assert user is not None

    session.execute.return_value = SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: [1, "2"]))
    ids = await repo.get_user_ids_by_email(email=" USER@example.com ", limit=0)
    assert ids == [1, 2]
    assert await repo.get_user_ids_by_email(email="  ") == []

    await repo.upsert(user_id=1, profile={"email": "x@y.z", "summ": 10})
    await repo.delete_by_user_id(user_id=1)
    assert session.execute.await_count >= 4


@pytest.mark.asyncio
async def test_listmonk_users_repository_paths() -> None:
    session = MagicMock()
    session.execute = AsyncMock()
    session.add = MagicMock()
    repo = ListmonkUsersRepository(session)

    await repo.set_consent_pending(user_id=1)

    session.execute.return_value = SimpleNamespace(scalar_one_or_none=lambda: "row")
    assert await repo.get_by_user_id(user_id=1) == "row"
    session.execute.return_value = SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: ["row"]))
    assert await repo.get_by_subscriber_id(subscriber_id=10) == "row"
    session.execute.return_value = SimpleNamespace(
        scalars=lambda: SimpleNamespace(
            all=lambda: [
                SimpleNamespace(user_id=2),
                SimpleNamespace(user_id=1),
            ]
        )
    )
    assert (await repo.get_by_subscriber_id(subscriber_id=10)).user_id == 2

    await repo.upsert(
        user_id=1,
        subscriber_id=10,
        email="u@example.com",
        status="enabled",
        list_ids=[1, 2],
        attributes={"user_id": 1},
    )

    session.execute.return_value = SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: ["a", "b"]))
    assert await repo.get_pending_batch(limit=10) == ["a", "b"]
    assert await repo.get_batch_after_user_id(last_user_id=1, limit=10) == ["a", "b"]

    await repo.mark_checked(user_id=1, pending=False, confirmed=True, status="confirmed")
    await repo.mark_checked(user_id=2, pending=True, confirmed=False)
    await repo.delete_by_user_id(user_id=1)


@pytest.mark.asyncio
async def test_merge_log_repository_paths() -> None:
    session = MagicMock()
    session.execute = AsyncMock()
    session.add = MagicMock()
    repo = MergeLogRepository(session)

    session.execute.return_value = SimpleNamespace(scalar_one_or_none=lambda: 1)
    assert await repo.exists(user_id=1) is True
    session.execute.return_value = SimpleNamespace(scalar_one_or_none=lambda: None)
    assert await repo.exists(user_id=2) is False

    await repo.create(user_id=1, source_event_type="CREATE", source_event_id="e", trace_id="t")
    session.add.assert_called_once()
    added = session.add.call_args.args[0]
    assert isinstance(added, MergeLog)
    assert added.user_id == 1

    await repo.delete_by_user_id(user_id=1)


@pytest.mark.asyncio
async def test_bonus_accrual_repository_paths() -> None:
    session = AsyncMock()
    repo = BonusAccrualRepository(session)

    session.execute.return_value = SimpleNamespace(rowcount=1)
    assert (
        await repo.reserve(
            user_id=1,
            reason="consent",
            idempotency_key="k1",
            payload={"a": 1},
        )
        is True
    )
    session.execute.return_value = SimpleNamespace(rowcount=0)
    assert (
        await repo.reserve(
            user_id=1,
            reason="consent",
            idempotency_key="k1",
            payload={"a": 1},
        )
        is False
    )

    await repo.mark_done(idempotency_key="k1")
    await repo.save_progress(idempotency_key="k1", payload={"bonus_done": True})
    await repo.mark_done_with_payload(idempotency_key="k1", payload={"key1_done": True})
    await repo.mark_failed(idempotency_key="k1", error_text="boom")

    session.execute.return_value = SimpleNamespace(scalar_one_or_none=lambda: "row")
    assert await repo.get_by_key(idempotency_key="k1") == "row"

    await repo.delete_by_user_id(user_id=1)


@pytest.mark.asyncio
async def test_sync_state_repository_paths() -> None:
    session = AsyncMock()
    repo = SyncStateRepository(session)

    session.execute.side_effect = [
        SimpleNamespace(scalar_one_or_none=lambda: "existing"),
    ]
    assert await repo.get_or_create(source="s", list_id=1) == "existing"

    session.execute.side_effect = [
        SimpleNamespace(scalar_one_or_none=lambda: None),
        SimpleNamespace(),
        SimpleNamespace(scalar_one_or_none=lambda: "created"),
    ]
    assert await repo.get_or_create(source="s", list_id=2) == "created"

    session.execute.side_effect = [
        SimpleNamespace(scalar_one_or_none=lambda: None),
        SimpleNamespace(),
        SimpleNamespace(scalar_one_or_none=lambda: None),
    ]
    with pytest.raises(RuntimeError):
        await repo.get_or_create(source="s", list_id=3)

    session.execute.side_effect = None
    session.execute.return_value = SimpleNamespace()

    await repo.update_watermark(
        source="s",
        list_id=1,
        updated_at=None,
        subscriber_id=1,
    )
