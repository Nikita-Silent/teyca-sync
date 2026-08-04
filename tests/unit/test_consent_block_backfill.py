from __future__ import annotations

from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.config import Settings
from app.workers.consent_block_backfill import (
    BLOCKED_STATUSES,
    ConsentBlockBackfill,
    ConsentBlockCandidate,
)


def _backfill() -> ConsentBlockBackfill:
    return ConsentBlockBackfill(
        settings=cast(Settings, SimpleNamespace()),
        session_factory=MagicMock(),
    )


async def _run_operation_directly(operation: object) -> object:
    return await operation(AsyncMock())  # type: ignore[misc]


@pytest.mark.asyncio
async def test_collect_candidates_returns_every_blocked_user() -> None:
    backfill = _backfill()
    listmonk_repo = AsyncMock()
    listmonk_repo.get_by_statuses.return_value = [
        SimpleNamespace(user_id=1),
        SimpleNamespace(user_id=2),
    ]

    with (
        patch(
            "app.workers.consent_block_backfill.ListmonkUsersRepository",
            return_value=listmonk_repo,
        ),
        patch.object(
            ConsentBlockBackfill,
            "_run_in_session",
            new=AsyncMock(side_effect=_run_operation_directly),
        ),
    ):
        candidates = await backfill.collect_candidates()

    listmonk_repo.get_by_statuses.assert_awaited_once_with(statuses=list(BLOCKED_STATUSES))
    assert candidates == [ConsentBlockCandidate(user_id=1), ConsentBlockCandidate(user_id=2)]


@pytest.mark.asyncio
async def test_enqueue_counts_new_and_already_queued() -> None:
    backfill = _backfill()
    candidates = [ConsentBlockCandidate(user_id=1), ConsentBlockCandidate(user_id=2)]

    with patch.object(
        ConsentBlockBackfill,
        "_enqueue_one",
        new=AsyncMock(side_effect=[True, False]),
    ) as enqueue_one:
        summary = await backfill.enqueue(candidates=candidates)

    assert summary.candidates == 2
    assert summary.enqueued == 1
    assert summary.already_queued == 1
    assert enqueue_one.await_count == 2


@pytest.mark.asyncio
async def test_enqueue_one_uses_dedupe_key_and_payload() -> None:
    backfill = _backfill()
    outbox_repo = AsyncMock()
    outbox_repo.enqueue_once.return_value = True

    with (
        patch(
            "app.workers.consent_block_backfill.ExternalCallOutboxRepository",
            return_value=outbox_repo,
        ),
        patch.object(
            ConsentBlockBackfill,
            "_run_in_session",
            new=AsyncMock(side_effect=_run_operation_directly),
        ),
    ):
        created = await backfill._enqueue_one(user_id=42)

    assert created is True
    outbox_repo.enqueue_once.assert_awaited_once_with(
        operation="teyca_block_consent",
        dedupe_key="consent-block:42",
        user_id=42,
        payload={"status": "blocked"},
        trace_id="consent-block-backfill:42",
        source_event_id="consent-block-backfill:42",
        queue_name=None,
    )


def test_build_consent_block_backfill() -> None:
    from app.workers.consent_block_backfill import build_consent_block_backfill

    with patch(
        "app.workers.consent_block_backfill.get_settings",
        return_value=SimpleNamespace(),
    ):
        backfill = build_consent_block_backfill()
    assert backfill is not None
