from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.clients.listmonk import SubscriberProfile
from app.workers.listmonk_duplicate_subscriber_worker import (
    ListmonkDuplicateSubscriberWorker,
    _extract_authoritative_user_id,
    build_listmonk_duplicate_subscriber_worker,
)


def _worker() -> ListmonkDuplicateSubscriberWorker:
    session = AsyncMock()
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = session
    context_manager.__aexit__.return_value = False
    session_factory = MagicMock(return_value=context_manager)
    return ListmonkDuplicateSubscriberWorker(
        settings=SimpleNamespace(consent_sync_batch_size=100),
        session_factory=session_factory,
        listmonk_client=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_run_once_repairs_duplicate_subscriber_and_archives_losers() -> None:
    worker = _worker()
    session = worker.session_factory.return_value.__aenter__.return_value

    async def get_subscriber_profile(*, subscriber_id: int) -> SubscriberProfile:
        assert subscriber_id == 777
        assert session.commit.await_count >= 2
        return SubscriberProfile(
            subscriber_id=777,
            email="winner@example.com",
            status="confirmed",
            list_ids=[2, 5],
            attributes={"user_id": "20"},
        )

    worker.listmonk_client.get_subscriber_profile.side_effect = get_subscriber_profile

    with (
        patch(
            "app.workers.listmonk_duplicate_subscriber_worker.ListmonkUsersRepository"
        ) as listmonk_repo_cls,
        patch(
            "app.workers.listmonk_duplicate_subscriber_worker.ListmonkUserArchiveRepository"
        ) as archive_repo_cls,
    ):
        listmonk_repo = AsyncMock()
        listmonk_repo.get_duplicate_subscriber_ids.return_value = [777]
        listmonk_repo.get_rows_by_subscriber_id.return_value = [
            SimpleNamespace(user_id=10, subscriber_id=777, email=None, status="blocked"),
            SimpleNamespace(
                user_id=20, subscriber_id=777, email="winner@example.com", status="confirmed"
            ),
        ]
        listmonk_repo_cls.return_value = listmonk_repo

        archive_repo = AsyncMock()
        archive_repo_cls.return_value = archive_repo

        repaired = await worker.run_once()

    assert repaired == 1
    archive_repo.archive_loser.assert_awaited_once()
    listmonk_repo.delete_by_user_ids.assert_awaited_once_with(user_ids=[10])


@pytest.mark.asyncio
async def test_run_once_marks_manual_review_when_authoritative_user_missing() -> None:
    worker = _worker()
    session = worker.session_factory.return_value.__aenter__.return_value

    async def get_subscriber_profile(*, subscriber_id: int) -> SubscriberProfile:
        assert subscriber_id == 777
        assert session.commit.await_count >= 2
        return SubscriberProfile(
            subscriber_id=777,
            email="winner@example.com",
            status="confirmed",
            list_ids=[2, 5],
            attributes={},
        )

    worker.listmonk_client.get_subscriber_profile.side_effect = get_subscriber_profile

    with (
        patch(
            "app.workers.listmonk_duplicate_subscriber_worker.ListmonkUsersRepository"
        ) as listmonk_repo_cls,
        patch(
            "app.workers.listmonk_duplicate_subscriber_worker.ListmonkUserArchiveRepository"
        ) as archive_repo_cls,
    ):
        listmonk_repo = AsyncMock()
        listmonk_repo.get_duplicate_subscriber_ids.return_value = [777]
        listmonk_repo.get_rows_by_subscriber_id.return_value = [
            SimpleNamespace(user_id=10, subscriber_id=777, email=None, status="blocked"),
            SimpleNamespace(
                user_id=20, subscriber_id=777, email="winner@example.com", status="confirmed"
            ),
        ]
        listmonk_repo_cls.return_value = listmonk_repo

        archive_repo = AsyncMock()
        archive_repo_cls.return_value = archive_repo

        repaired = await worker.run_once()

    assert repaired == 0
    archive_repo.archive_loser.assert_not_awaited()
    listmonk_repo.delete_by_user_ids.assert_not_awaited()


def test_build_and_extract_authoritative_user_id() -> None:
    with (
        patch(
            "app.workers.listmonk_duplicate_subscriber_worker.get_settings",
            return_value=SimpleNamespace(),
        ),
        patch("app.workers.listmonk_duplicate_subscriber_worker.ListmonkSDKClient"),
    ):
        worker = build_listmonk_duplicate_subscriber_worker()
    assert worker is not None

    assert _extract_authoritative_user_id(None) is None
    assert (
        _extract_authoritative_user_id(
            SubscriberProfile(
                subscriber_id=1,
                email=None,
                status="enabled",
                list_ids=[],
                attributes={"user_id": "42"},
            )
        )
        == 42
    )
