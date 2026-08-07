"""teyca-sync-y1c: mark stale email_repair_log pending rows without calling Teyca.

email_repair_worker.py was never scheduled, so rows recorded before the
y1c policy backfill shipped stayed pending even after their underlying
duplicate group was resolved under a different row. This cleanup never
calls Teyca — it only checks whether the conflict still exists in `users`.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from service_workers.stale_pending_repair_cleanup import (
    StalePendingCandidate,
    StalePendingRepairCleanup,
    build_stale_pending_repair_cleanup,
)


def _cleanup() -> tuple[StalePendingRepairCleanup, MagicMock]:
    session_factory = MagicMock()
    cleanup = StalePendingRepairCleanup(session_factory=session_factory)
    return cleanup, session_factory


@pytest.mark.asyncio
async def test_collect_maps_stale_rows_to_candidates() -> None:
    cleanup, session_factory = _cleanup()
    session = AsyncMock()
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    session_factory.return_value = session_cm

    repair_repo = AsyncMock()
    repair_repo.get_stale_pending_batch.return_value = [
        SimpleNamespace(
            id=1, normalized_email="dup@example.com", incoming_user_id=10, existing_user_id=20
        ),
    ]

    with patch(
        "service_workers.stale_pending_repair_cleanup.EmailRepairLogRepository",
        return_value=repair_repo,
    ):
        candidates = await cleanup.collect(batch_size=200)

    repair_repo.get_stale_pending_batch.assert_awaited_once_with(limit=200)
    assert candidates == [
        StalePendingCandidate(
            repair_id=1,
            normalized_email="dup@example.com",
            incoming_user_id=10,
            existing_user_id=20,
        )
    ]


@pytest.mark.asyncio
async def test_apply_marks_every_candidate_stale_and_commits() -> None:
    cleanup, session_factory = _cleanup()
    session = AsyncMock()
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    session_factory.return_value = session_cm

    repair_repo = AsyncMock()
    candidates = [
        StalePendingCandidate(
            repair_id=1, normalized_email="a@example.com", incoming_user_id=10, existing_user_id=20
        ),
        StalePendingCandidate(
            repair_id=2, normalized_email="b@example.com", incoming_user_id=11, existing_user_id=21
        ),
    ]

    with patch(
        "service_workers.stale_pending_repair_cleanup.EmailRepairLogRepository",
        return_value=repair_repo,
    ):
        summary = await cleanup.apply(candidates=candidates)

    assert summary.candidates == 2
    assert summary.marked_stale == 2
    assert repair_repo.mark_stale.await_count == 2
    repair_repo.mark_stale.assert_any_await(
        repair_id=1,
        reason=(
            "normalized_email no longer matches both users "
            "(teyca-sync-y1c stale-pending cleanup)"
        ),
    )
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_apply_with_no_candidates_is_a_noop() -> None:
    cleanup, session_factory = _cleanup()
    session = AsyncMock()
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    session_factory.return_value = session_cm

    with patch(
        "service_workers.stale_pending_repair_cleanup.EmailRepairLogRepository",
        return_value=AsyncMock(),
    ):
        summary = await cleanup.apply(candidates=[])

    assert summary.candidates == 0
    assert summary.marked_stale == 0
    session.commit.assert_awaited_once()


def test_build_stale_pending_repair_cleanup() -> None:
    cleanup = build_stale_pending_repair_cleanup()
    assert cleanup is not None
