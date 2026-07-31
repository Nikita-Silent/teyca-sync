"""Repository for the Postgres-backed outgoing Teyca call budget (teyca-sync-3al)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import Select, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import TeycaCallBudget

# (window_kind, window_seconds, max_requests)
BudgetLimits = tuple[tuple[str, int, int], ...]


@dataclass(slots=True)
class BudgetReservation:
    """Outcome of a reservation attempt against all configured windows."""

    allowed: bool
    retry_after_seconds: float


class TeycaCallBudgetRepository:
    """Data access for teyca_call_budget rows."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def _get_locked_or_create(self, *, window_kind: str, now: datetime) -> TeycaCallBudget:
        stmt: Select[tuple[TeycaCallBudget]] = (
            select(TeycaCallBudget)
            .where(TeycaCallBudget.window_kind == window_kind)
            .with_for_update()
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is not None:
            return row

        insert_stmt = (
            insert(TeycaCallBudget)
            .values(window_kind=window_kind, window_start=now, used_count=0)
            .on_conflict_do_nothing(index_elements=[TeycaCallBudget.window_kind])
        )
        await self._session.execute(insert_stmt)
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            raise RuntimeError(f"Unable to load teyca_call_budget row after insert: {window_kind}")
        return row

    async def try_reserve(
        self, *, limits: BudgetLimits, cost: int = 1, now: datetime | None = None
    ) -> BudgetReservation:
        """Atomically reserve `cost` units across every window, or none at all.

        Locks all configured rows (FOR UPDATE) before deciding, so concurrent
        dispatcher processes never overshoot a window even though this method
        commits itself (via the caller's transaction) once all windows agree.
        """
        moment = now or datetime.now(UTC)
        rows: list[tuple[TeycaCallBudget, int, bool]] = []
        retry_after = 0.0
        for window_kind, window_seconds, max_requests in limits:
            row = await self._get_locked_or_create(window_kind=window_kind, now=moment)
            window_end = row.window_start + timedelta(seconds=window_seconds)
            rolled_over = moment >= window_end
            used = 0 if rolled_over else row.used_count
            if used + cost > max_requests:
                effective_window_end = (
                    moment + timedelta(seconds=window_seconds) if rolled_over else window_end
                )
                remaining = (effective_window_end - moment).total_seconds()
                retry_after = max(retry_after, remaining, 0.0)
            rows.append((row, used, rolled_over))

        if retry_after > 0:
            return BudgetReservation(allowed=False, retry_after_seconds=retry_after)

        for row, used, rolled_over in rows:
            if rolled_over:
                row.window_start = moment
            row.used_count = used + cost
            row.updated_at = moment
        return BudgetReservation(allowed=True, retry_after_seconds=0.0)

    async def get_remaining(self, *, limits: BudgetLimits, now: datetime | None = None) -> int:
        """Best-effort read of the tightest remaining budget across windows.

        Does not lock or mutate rows — used to cap how many outbox rows a
        dispatcher tick even attempts to claim (teyca-sync-3al), not to
        enforce the limit precisely (that happens in `try_reserve` per call).
        """
        moment = now or datetime.now(UTC)
        remaining_values: list[int] = []
        for window_kind, window_seconds, max_requests in limits:
            stmt: Select[tuple[TeycaCallBudget]] = select(TeycaCallBudget).where(
                TeycaCallBudget.window_kind == window_kind
            )
            result = await self._session.execute(stmt)
            row = result.scalar_one_or_none()
            if row is None:
                remaining_values.append(max_requests)
                continue
            window_end = row.window_start + timedelta(seconds=window_seconds)
            used = 0 if moment >= window_end else row.used_count
            remaining_values.append(max(0, max_requests - used))
        return min(remaining_values) if remaining_values else 0
