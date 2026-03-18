"""Read-only repository for historical data in old DB."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


class OldDBRepositoryError(Exception):
    """Raised when reading old DB fails."""


@dataclass(slots=True)
class OldUserData:
    """Historical user aggregates used for merge."""

    bonus: float | None = None
    summ: float | None = None
    summ_all: float | None = None
    summ_last: float | None = None
    check_summ: float | None = None
    visits: int | None = None
    visits_all: int | None = None

    def has_merge_data(self) -> bool:
        """Return True when at least one non-zero historical field is present."""
        return any(
            value not in (None, 0, 0.0)
            for value in (
                self.bonus,
                self.summ,
                self.summ_all,
                self.summ_last,
                self.check_summ,
                self.visits,
                self.visits_all,
            )
        )


class OldDBRepository:
    """Query old DB by normalized phone."""

    def __init__(self, export_db_url: str, *, request_timeout_seconds: float = 15.0) -> None:
        self._url = export_db_url.strip()
        self._request_timeout_seconds = max(0.1, float(request_timeout_seconds))
        self._engine: AsyncEngine | None = None
        self._columns_cache: set[str] | None = None

    async def get_user_data(self, *, phone: str | None = None) -> OldUserData | None:
        """Load historical aggregates from old DB users table."""
        if not self._url:
            return None

        if self._engine is None:
            self._engine = create_async_engine(
                self._url,
                pool_pre_ping=True,
                connect_args={
                    "timeout": self._request_timeout_seconds,
                    "command_timeout": self._request_timeout_seconds,
                },
            )

        available_columns = await self._get_users_columns()
        if "phone" not in available_columns:
            return None

        # Old DB schema differs between installations, so we read available aliases.
        field_aliases: dict[str, tuple[str, ...]] = {
            "bonus": ("bonus", "balance"),
            "summ": ("summ",),
            "summ_all": ("summ_all",),
            "summ_last": ("summ_last", "average_check"),
            "check_summ": ("check_summ", "check_sum"),
            "visits": ("check_count", "visits"),
            "visits_all": ("visits_all",),
        }
        selected_columns = sorted(
            {
                alias
                for aliases in field_aliases.values()
                for alias in aliases
                if alias in available_columns
            }
        )
        if not selected_columns:
            return None

        phone_last10 = _normalize_phone_last10(phone)
        if phone_last10 is None:
            return None
        query_params: dict[str, object] = {"phone_last10": phone_last10}

        query = text(
            f"""
            SELECT {", ".join(selected_columns)}
            FROM users
            WHERE RIGHT(regexp_replace(COALESCE(phone, ''), '[^0-9]', '', 'g'), 10) = :phone_last10
            LIMIT 1
            """
        )
        try:
            async with self._engine.connect() as conn:
                result = await asyncio.wait_for(
                    conn.execute(query, query_params),
                    timeout=self._request_timeout_seconds,
                )
                row = result.mappings().first()
        except TimeoutError as exc:
            raise OldDBRepositoryError(
                f"Old DB query timeout after {self._request_timeout_seconds}s"
            ) from exc
        except SQLAlchemyError as exc:
            raise OldDBRepositoryError("Failed reading old DB by phone") from exc

        if row is None:
            return None
        return OldUserData(
            bonus=_to_optional_scaled_float(_pick_first(row, "bonus", "balance")),
            summ=_to_optional_float(_pick_first(row, "summ")),
            summ_all=_to_optional_float(_pick_first(row, "summ_all")),
            summ_last=_to_optional_float(_pick_first(row, "summ_last", "average_check")),
            check_summ=_to_optional_float(_pick_first(row, "check_summ", "check_sum")),
            visits=_to_optional_int(_pick_first(row, "check_count", "visits")),
            visits_all=_to_optional_int(_pick_first(row, "visits_all")),
        )

    async def close(self) -> None:
        """Dispose old DB engine if it was initialized."""
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None
            self._columns_cache = None

    async def _get_users_columns(self) -> set[str]:
        if self._columns_cache is not None:
            return self._columns_cache
        if self._engine is None:
            self._columns_cache = set()
            return self._columns_cache

        query = text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'users'
            """
        )
        try:
            async with self._engine.connect() as conn:
                result = await asyncio.wait_for(
                    conn.execute(query),
                    timeout=self._request_timeout_seconds,
                )
                self._columns_cache = {str(row[0]) for row in result.fetchall()}
        except TimeoutError:
            self._columns_cache = set()
        except SQLAlchemyError:
            self._columns_cache = set()
        return self._columns_cache


def _to_optional_float(raw: object) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, (float, int)):
        return float(raw)
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _to_optional_scaled_float(raw: object) -> float | None:
    value = _to_optional_float(raw)
    if value is None:
        return None
    # Old DB stores monetary/bonus values in minor units; divide by 100 without remainder.
    return float(int(value) // 100)


def _to_optional_int(raw: object) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        return int(raw)
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            return None
        try:
            return int(stripped)
        except ValueError:
            return None
    return None


def _pick_first(row: Any, *keys: str) -> object:
    for key in keys:
        if key in row:
            value = row.get(key)
            if value is not None:
                return value
    return None


def _normalize_phone_last10(phone: str | None) -> str | None:
    if phone is None:
        return None
    digits = "".join(ch for ch in phone if ch.isdigit())
    if len(digits) < 10:
        return None
    return digits[-10:]
