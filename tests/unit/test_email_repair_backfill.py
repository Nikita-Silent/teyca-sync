from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from app.config import Settings
from app.workers.email_repair_backfill import (
    DuplicateEmailBackfill,
    DuplicateEmailBackfillError,
    DuplicateEmailBackfillIssue,
    DuplicateEmailBackfillPlan,
)


@dataclass(slots=True)
class _BackfillMocks:
    session_factory: MagicMock
    listmonk_client: AsyncMock
    teyca_client: AsyncMock


def _backfill() -> tuple[DuplicateEmailBackfill, _BackfillMocks]:
    mocks = _BackfillMocks(
        session_factory=MagicMock(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
    )
    backfill = DuplicateEmailBackfill(
        settings=cast(Settings, SimpleNamespace()),
        session_factory=mocks.session_factory,
        listmonk_client=mocks.listmonk_client,
        teyca_client=mocks.teyca_client,
    )
    return backfill, mocks


@pytest.mark.asyncio
async def test_collect_plans_resolves_winner_via_listmonk_truth() -> None:
    backfill, mocks = _backfill()
    session = AsyncMock()
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    mocks.session_factory.return_value = session_cm

    listmonk_repo = AsyncMock()
    listmonk_repo.get_duplicate_emails.return_value = ["dup@example.com"]
    listmonk_repo.get_by_email.return_value = [
        SimpleNamespace(user_id=10),
        SimpleNamespace(user_id=20),
    ]
    listmonk_repo.get_by_subscriber_id.return_value = SimpleNamespace(user_id=20)
    mocks.listmonk_client.get_subscriber_by_email.return_value = SimpleNamespace(subscriber_id=777)

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
    backfill, mocks = _backfill()
    session = AsyncMock()
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    mocks.session_factory.return_value = session_cm

    listmonk_repo = AsyncMock()
    listmonk_repo.get_duplicate_emails.return_value = ["dup@example.com"]
    listmonk_repo.get_by_email.return_value = [
        SimpleNamespace(user_id=10),
        SimpleNamespace(user_id=20),
    ]
    mocks.listmonk_client.get_subscriber_by_email.return_value = None

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
async def test_collect_plans_via_policy_uses_37z_phone_date_rules() -> None:
    """teyca-sync-y1c: the one-time cleanup resolves users.email duplicates via
    the deterministic Р5/Р6 policy (teyca-sync-37z), not Listmonk truth."""
    backfill, mocks = _backfill()
    session = AsyncMock()
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    mocks.session_factory.return_value = session_cm

    users_repo = AsyncMock()
    users_repo.get_duplicate_email_groups.return_value = [
        (
            "same-phone@example.com",
            [
                SimpleNamespace(user_id=1, phone="+7900", date_last="2026-01-01", updated_at=None),
                SimpleNamespace(user_id=2, phone="+7900", date_last="2026-07-01", updated_at=None),
            ],
        ),
        (
            "diff-phone@example.com",
            [
                SimpleNamespace(user_id=3, phone="+7901", date_last="2026-01-01", updated_at=None),
                SimpleNamespace(user_id=4, phone="+7902", date_last="2026-07-01", updated_at=None),
            ],
        ),
    ]

    with patch("app.workers.email_repair_backfill.UsersRepository", return_value=users_repo):
        plans, issues = await backfill.collect_plans_via_policy()

    assert issues == []
    assert plans == [
        DuplicateEmailBackfillPlan(
            normalized_email="same-phone@example.com",
            winner_user_id=2,
            winner_subscriber_id=None,
            loser_user_ids=[1],
            mark_bad_email=False,
        ),
        DuplicateEmailBackfillPlan(
            normalized_email="diff-phone@example.com",
            winner_user_id=4,
            winner_subscriber_id=None,
            loser_user_ids=[3],
            mark_bad_email=True,
        ),
    ]


@pytest.mark.asyncio
async def test_apply_clears_loser_emails_and_persists_db_applied_rows() -> None:
    backfill, mocks = _backfill()
    session = AsyncMock()
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    mocks.session_factory.return_value = session_cm

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
async def test_apply_threads_mark_bad_email_flag_into_repair_log() -> None:
    backfill, mocks = _backfill()
    session = AsyncMock()
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    mocks.session_factory.return_value = session_cm

    repair_repo = AsyncMock()
    plan = DuplicateEmailBackfillPlan(
        normalized_email="dup@example.com",
        winner_user_id=20,
        winner_subscriber_id=None,
        loser_user_ids=[10],
        mark_bad_email=False,
    )

    with (
        patch("app.workers.email_repair_backfill.UsersRepository", return_value=AsyncMock()),
        patch(
            "app.workers.email_repair_backfill.ListmonkUsersRepository", return_value=AsyncMock()
        ),
        patch(
            "app.workers.email_repair_backfill.EmailRepairLogRepository", return_value=repair_repo
        ),
    ):
        await backfill.apply(plans=[plan], issues=[])

    repair_repo.create_db_applied.assert_awaited_once_with(
        normalized_email="dup@example.com",
        incoming_user_id=10,
        existing_user_id=20,
        winner_user_id=20,
        winner_subscriber_id=None,
        source_event_id=repair_repo.create_db_applied.await_args.kwargs["source_event_id"],
        trace_id=repair_repo.create_db_applied.await_args.kwargs["trace_id"],
        mark_bad_email=False,
    )


@pytest.mark.asyncio
async def test_apply_rejects_partial_execution_when_issues_exist() -> None:
    backfill, _ = _backfill()

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
    backfill, mocks = _backfill()
    rows = [
        SimpleNamespace(
            id=1,
            incoming_user_id=10,
            winner_user_id=20,
            winner_subscriber_id=777,
            attempts=0,
            mark_bad_email=True,
        )
    ]

    with (
        patch.object(
            DuplicateEmailBackfill, "_load_db_applied_rows", new=AsyncMock(return_value=rows)
        ),
        patch.object(
            DuplicateEmailBackfill, "_mark_teyca_synced", new=AsyncMock()
        ) as mark_teyca_synced,
    ):
        summary = await backfill.sync_teyca(batch_size=10)

    assert summary.teyca_synced == 1
    assert mocks.teyca_client.update_pass_fields.await_count == 2
    mocks.teyca_client.update_pass_fields.assert_any_await(
        user_id=20,
        fields={"key6": "bugs"},
    )
    mocks.teyca_client.update_pass_fields.assert_any_await(
        user_id=10,
        fields={"email": None, "key1": "bad email", "key6": "bugs"},
    )
    mark_teyca_synced.assert_awaited_once_with(
        repair_id=1,
        winner_user_id=20,
        winner_subscriber_id=777,
    )


@pytest.mark.asyncio
async def test_sync_teyca_same_person_loser_skips_bad_email_mark() -> None:
    """Р5/Р6 (teyca-sync-37z): same-phone losers are cleared without key1=bad email."""
    backfill, mocks = _backfill()
    rows = [
        SimpleNamespace(
            id=2,
            incoming_user_id=11,
            winner_user_id=21,
            winner_subscriber_id=None,
            attempts=0,
            mark_bad_email=False,
        )
    ]

    with (
        patch.object(
            DuplicateEmailBackfill, "_load_db_applied_rows", new=AsyncMock(return_value=rows)
        ),
        patch.object(
            DuplicateEmailBackfill, "_mark_teyca_synced", new=AsyncMock()
        ) as mark_teyca_synced,
    ):
        summary = await backfill.sync_teyca(batch_size=10)

    assert summary.teyca_synced == 1
    mocks.teyca_client.update_pass_fields.assert_any_await(
        user_id=11,
        fields={"email": None, "key6": "bugs"},
    )
    mark_teyca_synced.assert_awaited_once_with(
        repair_id=2,
        winner_user_id=21,
        winner_subscriber_id=None,
    )


@pytest.mark.asyncio
async def test_sync_teyca_marks_retry_on_teyca_error() -> None:
    backfill, mocks = _backfill()
    mocks.teyca_client.update_pass_fields.side_effect = httpx.ReadTimeout("boom")
    rows = [
        SimpleNamespace(
            id=1,
            incoming_user_id=10,
            winner_user_id=20,
            winner_subscriber_id=777,
            attempts=0,
            mark_bad_email=True,
        )
    ]

    with (
        patch.object(
            DuplicateEmailBackfill, "_load_db_applied_rows", new=AsyncMock(return_value=rows)
        ),
        patch.object(
            DuplicateEmailBackfill, "_mark_retry", new=AsyncMock(return_value="failed")
        ) as mark_retry,
    ):
        summary = await backfill.sync_teyca(batch_size=10)

    assert summary.teyca_failed == 1
    mark_retry.assert_awaited_once_with(
        repair_id=1,
        attempts=1,
        error_text="boom",
    )
