from __future__ import annotations

from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.clients.teyca import TeycaAPIError
from app.config import Settings
from app.workers.consent_bonus_backfill import (
    BACKFILL_STATUSES,
    ConsentBonusBackfill,
    ConsentBonusBackfillError,
    consent_bonus_idempotency_key,
)


def _backfill(**settings_overrides: object) -> tuple[ConsentBonusBackfill, AsyncMock]:
    settings = {"consent_bonus_amount": "110.0", **settings_overrides}
    teyca_client = AsyncMock()
    backfill = ConsentBonusBackfill(
        settings=cast(Settings, SimpleNamespace(**settings)),
        session_factory=MagicMock(),
        teyca_client=teyca_client,
    )
    return backfill, teyca_client


async def _run_operation_directly(operation: object) -> object:
    return await operation(AsyncMock())  # type: ignore[misc]


def test_consent_bonus_idempotency_key() -> None:
    assert consent_bonus_idempotency_key(42) == "email_consent:42"


@pytest.mark.asyncio
async def test_collect_candidates_filters_invalid_duplicate_and_already_logged() -> None:
    backfill, _ = _backfill()
    rows = [
        SimpleNamespace(
            user_id=1, subscriber_id=101, email="valid@example.com", status="unconfirmed"
        ),
        SimpleNamespace(user_id=2, subscriber_id=102, email="not-an-email", status="enabled"),
        SimpleNamespace(
            user_id=3, subscriber_id=103, email="dup@example.com", status="unconfirmed"
        ),
        SimpleNamespace(
            user_id=4, subscriber_id=104, email="already-paid@example.com", status="enabled"
        ),
    ]

    listmonk_repo = AsyncMock()
    listmonk_repo.get_duplicate_emails.return_value = ["dup@example.com"]
    listmonk_repo.get_by_statuses.return_value = rows

    accrual_repo = AsyncMock()

    async def get_by_key(*, idempotency_key: str) -> object | None:
        if idempotency_key == "email_consent:4":
            return SimpleNamespace(status="done")
        return None

    accrual_repo.get_by_key.side_effect = get_by_key

    with (
        patch(
            "app.workers.consent_bonus_backfill.ListmonkUsersRepository",
            return_value=listmonk_repo,
        ),
        patch(
            "app.workers.consent_bonus_backfill.BonusAccrualRepository",
            return_value=accrual_repo,
        ),
        patch.object(
            ConsentBonusBackfill,
            "_run_in_session",
            new=AsyncMock(side_effect=_run_operation_directly),
        ),
    ):
        candidates = await backfill.collect_candidates()

    listmonk_repo.get_by_statuses.assert_awaited_once_with(statuses=list(BACKFILL_STATUSES))
    assert [c.user_id for c in candidates] == [1]
    assert candidates[0].email == "valid@example.com"


@pytest.mark.asyncio
async def test_reconcile_with_teyca_matches_by_value_only() -> None:
    backfill, teyca_client = _backfill(consent_bonus_amount="110.0")
    teyca_client.list_operations.return_value = [
        {"user_id": 1, "value": "110.0"},
        {"user_id": 2, "value": "5.0"},
        {"user_id": "3", "value": "110.0"},
        {"user_id": 4},
    ]
    candidates = [
        SimpleNamespace(user_id=1),
        SimpleNamespace(user_id=2),
        SimpleNamespace(user_id=3),
        SimpleNamespace(user_id=4),
    ]

    matched = await backfill.reconcile_with_teyca(candidates=cast(list, candidates))

    assert matched == {1, 3}
    teyca_client.list_operations.assert_awaited_once()


@pytest.mark.asyncio
async def test_reconcile_with_teyca_returns_empty_without_candidates() -> None:
    backfill, teyca_client = _backfill()
    assert await backfill.reconcile_with_teyca(candidates=[]) == set()
    teyca_client.list_operations.assert_not_awaited()


@pytest.mark.asyncio
async def test_accrue_skips_operations_match_and_pays_the_rest() -> None:
    backfill, teyca_client = _backfill()
    candidates = [SimpleNamespace(user_id=1), SimpleNamespace(user_id=2)]

    with patch.object(
        ConsentBonusBackfill, "_accrue_one", new=AsyncMock()
    ) as accrue_one:
        summary = await backfill.accrue(
            candidates=cast(list, candidates),
            already_paid={1},
        )

    assert summary.candidates == 2
    assert summary.skipped_operations_match == 1
    assert summary.accrued == 1
    assert summary.failed == 0
    accrue_one.assert_awaited_once_with(user_id=2)


@pytest.mark.asyncio
async def test_accrue_counts_teyca_failures_without_stopping_batch() -> None:
    backfill, _ = _backfill()
    candidates = [SimpleNamespace(user_id=1), SimpleNamespace(user_id=2)]

    with patch.object(
        ConsentBonusBackfill,
        "_accrue_one",
        new=AsyncMock(side_effect=[TeycaAPIError("boom"), None]),
    ):
        summary = await backfill.accrue(candidates=cast(list, candidates), already_paid=set())

    assert summary.accrued == 1
    assert summary.failed == 1


@pytest.mark.asyncio
async def test_accrue_one_runs_both_steps_and_marks_done() -> None:
    backfill, teyca_client = _backfill()
    accrual_repo = AsyncMock()
    accrual_repo.get_by_key.return_value = SimpleNamespace(
        payload={"bonus_done": False, "key1_done": False}
    )

    with (
        patch(
            "app.workers.consent_bonus_backfill.BonusAccrualRepository",
            return_value=accrual_repo,
        ),
        patch.object(
            ConsentBonusBackfill,
            "_run_in_session",
            new=AsyncMock(side_effect=_run_operation_directly),
        ),
    ):
        await backfill._accrue_one(user_id=7)

    teyca_client.accrue_bonuses.assert_awaited_once()
    teyca_client.update_pass_fields.assert_awaited_once_with(
        user_id=7, fields={"key1": "confirmed"}
    )
    accrual_repo.reserve.assert_awaited_once()
    assert accrual_repo.save_progress.await_count == 2
    done_payload = accrual_repo.mark_done_with_payload.await_args.kwargs["payload"]
    assert done_payload == {"bonus_done": True, "key1_done": True}


@pytest.mark.asyncio
async def test_accrue_one_resumes_only_remaining_step() -> None:
    backfill, teyca_client = _backfill()
    accrual_repo = AsyncMock()
    accrual_repo.get_by_key.return_value = SimpleNamespace(
        payload={"bonus_done": True, "key1_done": False}
    )

    with (
        patch(
            "app.workers.consent_bonus_backfill.BonusAccrualRepository",
            return_value=accrual_repo,
        ),
        patch.object(
            ConsentBonusBackfill,
            "_run_in_session",
            new=AsyncMock(side_effect=_run_operation_directly),
        ),
    ):
        await backfill._accrue_one(user_id=8)

    teyca_client.accrue_bonuses.assert_not_awaited()
    teyca_client.update_pass_fields.assert_awaited_once()


@pytest.mark.asyncio
async def test_accrue_one_raises_when_reserve_row_missing() -> None:
    backfill, _teyca_client = _backfill()
    accrual_repo = AsyncMock()
    accrual_repo.get_by_key.return_value = None

    with (
        patch(
            "app.workers.consent_bonus_backfill.BonusAccrualRepository",
            return_value=accrual_repo,
        ),
        patch.object(
            ConsentBonusBackfill,
            "_run_in_session",
            new=AsyncMock(side_effect=_run_operation_directly),
        ),
    ):
        with pytest.raises(ConsentBonusBackfillError):
            await backfill._accrue_one(user_id=9)


def test_to_optional_int_and_str_helpers() -> None:
    from app.workers.consent_bonus_backfill import _to_optional_int, _to_optional_str

    assert _to_optional_int(True) is None
    assert _to_optional_int(5) == 5
    assert _to_optional_int("42") == 42
    assert _to_optional_int("not-a-number") is None
    assert _to_optional_str(True) is None
    assert _to_optional_str(110.0) == "110.0"
    assert _to_optional_str(None) is None


def test_build_consent_bonus_backfill() -> None:
    from app.workers.consent_bonus_backfill import build_consent_bonus_backfill

    with (
        patch(
            "app.workers.consent_bonus_backfill.get_settings",
            return_value=SimpleNamespace(),
        ),
        patch("app.workers.consent_bonus_backfill.build_teyca_client"),
    ):
        backfill = build_consent_bonus_backfill()
    assert backfill is not None
