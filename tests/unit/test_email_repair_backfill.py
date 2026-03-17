from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from app.workers.email_repair_backfill import (
    DuplicateEmailBackfill,
    DuplicateEmailBackfillError,
    DuplicateEmailBackfillIssue,
    DuplicateEmailBackfillPlan,
)


def _backfill() -> DuplicateEmailBackfill:
    session_factory = MagicMock()
    return DuplicateEmailBackfill(
        settings=SimpleNamespace(),
        session_factory=session_factory,
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_collect_plans_resolves_winner_via_listmonk_truth() -> None:
    backfill = _backfill()
    session = AsyncMock()
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    backfill.session_factory.return_value = session_cm

    listmonk_repo = AsyncMock()
    listmonk_repo.get_duplicate_emails.return_value = ["dup@example.com"]
    listmonk_repo.get_by_email.return_value = [
        SimpleNamespace(user_id=10),
        SimpleNamespace(user_id=20),
    ]
    listmonk_repo.get_by_subscriber_id.return_value = SimpleNamespace(user_id=20)
    backfill.listmonk_client.get_subscriber_by_email.return_value = SimpleNamespace(
        subscriber_id=777
    )

    with patch(
        "app.workers.email_repair_backfill.ListmonkUsersRepository", return_value=listmonk_repo
    ):
        plans, issues = await backfill.collect_plans()

    assert issues == []
    assert plans == [
        DuplicateEmailBackfillPlan(
            normalized_email="dup@example.com",
            winner_user_id=20,
            winner_subscriber_id=777,
            loser_user_ids=[10],
        )
    ]


@pytest.mark.asyncio
async def test_collect_plans_reports_unresolved_groups() -> None:
    backfill = _backfill()
    session = AsyncMock()
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    backfill.session_factory.return_value = session_cm

    listmonk_repo = AsyncMock()
    listmonk_repo.get_duplicate_emails.return_value = ["dup@example.com"]
    listmonk_repo.get_by_email.return_value = [
        SimpleNamespace(user_id=10),
        SimpleNamespace(user_id=20),
    ]
    backfill.listmonk_client.get_subscriber_by_email.return_value = None

    with patch(
        "app.workers.email_repair_backfill.ListmonkUsersRepository", return_value=listmonk_repo
    ):
        plans, issues = await backfill.collect_plans()

    assert plans == []
    assert issues == [
        DuplicateEmailBackfillIssue(
            normalized_email="dup@example.com",
            candidate_user_ids=[10, 20],
            error="subscriber_by_email returned no subscriber",
        )
    ]


@pytest.mark.asyncio
async def test_apply_clears_loser_emails_and_persists_db_applied_rows() -> None:
    backfill = _backfill()
    session = AsyncMock()
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    backfill.session_factory.return_value = session_cm

    users_repo = AsyncMock()
    listmonk_repo = AsyncMock()
    repair_repo = AsyncMock()
    plan = DuplicateEmailBackfillPlan(
        normalized_email="dup@example.com",
        winner_user_id=20,
        winner_subscriber_id=777,
        loser_user_ids=[10, 30],
    )

    with (
        patch("app.workers.email_repair_backfill.UsersRepository", return_value=users_repo),
        patch(
            "app.workers.email_repair_backfill.ListmonkUsersRepository", return_value=listmonk_repo
        ),
        patch(
            "app.workers.email_repair_backfill.EmailRepairLogRepository", return_value=repair_repo
        ),
    ):
        summary = await backfill.apply(plans=[plan], issues=[])

    assert summary.duplicate_emails == 1
    assert summary.loser_rows == 2
    users_repo.clear_email.assert_any_await(user_id=10)
    users_repo.clear_email.assert_any_await(user_id=30)
    listmonk_repo.clear_email.assert_any_await(user_id=10)
    listmonk_repo.clear_email.assert_any_await(user_id=30)
    assert repair_repo.create_db_applied.await_count == 2
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_apply_rejects_partial_execution_when_issues_exist() -> None:
    backfill = _backfill()

    with pytest.raises(DuplicateEmailBackfillError):
        await backfill.apply(
            plans=[],
            issues=[
                DuplicateEmailBackfillIssue(
                    normalized_email="dup@example.com",
                    candidate_user_ids=[10, 20],
                    error="boom",
                )
            ],
        )


@pytest.mark.asyncio
async def test_sync_teyca_marks_rows_synced() -> None:
    backfill = _backfill()
    session = AsyncMock()
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    backfill.session_factory.return_value = session_cm

    repair_repo = AsyncMock()
    repair_repo.get_db_applied_batch.return_value = [
        SimpleNamespace(
            id=1,
            incoming_user_id=10,
            winner_user_id=20,
            winner_subscriber_id=777,
            attempts=0,
        )
    ]

    with patch(
        "app.workers.email_repair_backfill.EmailRepairLogRepository", return_value=repair_repo
    ):
        summary = await backfill.sync_teyca(batch_size=10)

    assert summary.teyca_synced == 1
    backfill.teyca_client.update_pass_fields.assert_awaited_once_with(
        user_id=10,
        fields={"email": None, "key1": "bad email"},
    )
    repair_repo.mark_teyca_synced.assert_awaited_once_with(
        repair_id=1,
        winner_user_id=20,
        winner_subscriber_id=777,
    )
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_sync_teyca_marks_retry_on_teyca_error() -> None:
    backfill = _backfill()
    session = AsyncMock()
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    backfill.session_factory.return_value = session_cm
    backfill.teyca_client.update_pass_fields.side_effect = httpx.ReadTimeout("boom")

    repair_repo = AsyncMock()
    repair_repo.get_db_applied_batch.return_value = [
        SimpleNamespace(
            id=1,
            incoming_user_id=10,
            winner_user_id=20,
            winner_subscriber_id=777,
            attempts=0,
        )
    ]
    repair_repo.mark_retry.return_value = "failed"

    with patch(
        "app.workers.email_repair_backfill.EmailRepairLogRepository", return_value=repair_repo
    ):
        summary = await backfill.sync_teyca(batch_size=10)

    assert summary.teyca_failed == 1
    repair_repo.mark_retry.assert_awaited_once_with(
        repair_id=1,
        attempts=1,
        error_text="boom",
        max_attempts=3,
    )
