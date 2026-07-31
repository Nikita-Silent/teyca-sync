from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.db.models import TeycaCallBudget
from app.repositories.teyca_call_budget import TeycaCallBudgetRepository


def _row(*, window_kind: str, window_start: datetime, used_count: int) -> TeycaCallBudget:
    return TeycaCallBudget(
        window_kind=window_kind, window_start=window_start, used_count=used_count
    )


def _session_returning(rows: dict[str, TeycaCallBudget | None]) -> AsyncMock:
    """Mock session.execute returning `rows[window_kind]` for every SELECT.

    Good enough for `try_reserve`/`get_remaining`: both only ever SELECT by
    window_kind, so we can key canned results by that without parsing SQL.
    """
    session = AsyncMock()

    async def execute(stmt: object, *args: object, **kwargs: object) -> SimpleNamespace:
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        for window_kind, row in rows.items():
            if f"'{window_kind}'" in compiled or window_kind in compiled:
                return SimpleNamespace(scalar_one_or_none=lambda row=row: row)
        return SimpleNamespace(scalar_one_or_none=lambda: None)

    session.execute.side_effect = execute
    return session


@pytest.mark.asyncio
async def test_try_reserve_succeeds_under_limit_and_increments_all_windows() -> None:
    now = datetime(2026, 7, 31, 12, 0, 0, tzinfo=UTC)
    minute_row = _row(window_kind="minute", window_start=now - timedelta(seconds=10), used_count=5)
    hour_row = _row(window_kind="hour", window_start=now - timedelta(minutes=10), used_count=100)
    session = _session_returning({"minute": minute_row, "hour": hour_row})
    repo = TeycaCallBudgetRepository(session)

    result = await repo.try_reserve(
        limits=(("minute", 60, 50), ("hour", 3600, 500)),
        now=now,
    )

    assert result.allowed is True
    assert minute_row.used_count == 6
    assert hour_row.used_count == 101


@pytest.mark.asyncio
async def test_try_reserve_denies_when_any_window_is_full() -> None:
    now = datetime(2026, 7, 31, 12, 0, 0, tzinfo=UTC)
    minute_row = _row(window_kind="minute", window_start=now - timedelta(seconds=10), used_count=5)
    hour_row = _row(window_kind="hour", window_start=now - timedelta(minutes=10), used_count=500)
    session = _session_returning({"minute": minute_row, "hour": hour_row})
    repo = TeycaCallBudgetRepository(session)

    result = await repo.try_reserve(
        limits=(("minute", 60, 50), ("hour", 3600, 500)),
        now=now,
    )

    assert result.allowed is False
    assert result.retry_after_seconds > 0
    # Neither window's usage is touched when the reservation is denied.
    assert minute_row.used_count == 5
    assert hour_row.used_count == 500


@pytest.mark.asyncio
async def test_try_reserve_rolls_over_expired_window() -> None:
    now = datetime(2026, 7, 31, 12, 0, 0, tzinfo=UTC)
    # Window started 2 minutes ago but only lasts 60s — long expired, even
    # though used_count is sitting at the limit from the previous window.
    minute_row = _row(window_kind="minute", window_start=now - timedelta(minutes=2), used_count=50)
    session = _session_returning({"minute": minute_row})
    repo = TeycaCallBudgetRepository(session)

    result = await repo.try_reserve(limits=(("minute", 60, 50),), now=now)

    assert result.allowed is True
    assert minute_row.used_count == 1
    assert minute_row.window_start == now


@pytest.mark.asyncio
async def test_get_remaining_reflects_usage_without_mutating() -> None:
    now = datetime(2026, 7, 31, 12, 0, 0, tzinfo=UTC)
    minute_row = _row(window_kind="minute", window_start=now - timedelta(seconds=10), used_count=48)
    hour_row = _row(window_kind="hour", window_start=now - timedelta(minutes=10), used_count=100)
    session = _session_returning({"minute": minute_row, "hour": hour_row})
    repo = TeycaCallBudgetRepository(session)

    remaining = await repo.get_remaining(
        limits=(("minute", 60, 50), ("hour", 3600, 500)),
        now=now,
    )

    assert remaining == 2  # minute window is the tightest: 50 - 48
    assert minute_row.used_count == 48
    assert hour_row.used_count == 100


@pytest.mark.asyncio
async def test_get_remaining_treats_missing_row_as_full_budget() -> None:
    now = datetime(2026, 7, 31, 12, 0, 0, tzinfo=UTC)
    session = _session_returning({})
    repo = TeycaCallBudgetRepository(session)

    remaining = await repo.get_remaining(limits=(("minute", 60, 50),), now=now)

    assert remaining == 50
