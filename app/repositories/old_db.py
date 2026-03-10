"""Read-only repository for historical data in old DB."""

from __future__ import annotations

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
        """
        Indicates whether the instance contains any non-zero historical data field.
        
        Checks the following fields for a non-None, non-zero value: bonus, summ, summ_all, summ_last, check_summ, visits, visits_all.
        
        Returns:
            `true` if at least one of the listed fields is neither `None` nor zero, `false` otherwise.
        """
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

    def __init__(self, export_db_url: str) -> None:
        """
        Create a read-only repository configured with the given old database URL.
        
        Parameters:
            export_db_url (str): Connection URL for the old database; leading and trailing whitespace will be removed before use.
        """
        self._url = export_db_url.strip()
        self._engine: AsyncEngine | None = None
        self._columns_cache: set[str] | None = None

    async def get_user_data(self, *, phone: str | None = None) -> OldUserData | None:
        """
        Fetch historical aggregate fields for a user from the legacy database using the last 10 digits of the provided phone.
        
        Parameters:
            phone (str | None): Phone number to match; only the last 10 digits of the digit-only form are used. If None or fewer than 10 digits are present, the function returns None.
        
        Returns:
            OldUserData | None: An OldUserData instance populated with any available aggregate fields from the legacy users table, or `None` if no matching row is found or if the repository is not configured or the legacy schema lacks usable columns.
        
        Raises:
            OldDBRepositoryError: If the underlying database query fails.
        """
        if not self._url:
            return None

        if self._engine is None:
            self._engine = create_async_engine(self._url, pool_pre_ping=True)

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
            "visits": ("visits", "check_count"),
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
                result = await conn.execute(query, query_params)
                row = result.mappings().first()
        except SQLAlchemyError as exc:
            raise OldDBRepositoryError("Failed reading old DB by phone") from exc

        if row is None:
            return None
        return OldUserData(
            bonus=_to_optional_scaled_float(_pick_first(row, "bonus", "balance")),
            summ=_to_optional_scaled_float(_pick_first(row, "summ")),
            summ_all=_to_optional_scaled_float(_pick_first(row, "summ_all")),
            summ_last=_to_optional_scaled_float(_pick_first(row, "summ_last", "average_check")),
            check_summ=_to_optional_scaled_float(_pick_first(row, "check_summ", "check_sum")),
            visits=_to_optional_int(_pick_first(row, "visits", "check_count")),
            visits_all=_to_optional_int(_pick_first(row, "visits_all")),
        )

    async def close(self) -> None:
        """Dispose old DB engine if it was initialized."""
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None
            self._columns_cache = None

    async def _get_users_columns(self) -> set[str]:
        """
        Retrieve the cached set of column names for the public.users table, populating the cache by querying the database if it is not already cached.
        
        If the repository engine is not initialized or a database error occurs, the cache is set to and an empty set is returned.
        
        Returns:
            set[str]: Column name strings for the public.users table; empty set if the engine is not initialized or retrieval failed.
        """
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
                result = await conn.execute(query)
                self._columns_cache = {str(row[0]) for row in result.fetchall()}
        except SQLAlchemyError:
            self._columns_cache = set()
        return self._columns_cache


def _to_optional_float(raw: object) -> float | None:
    """
    Convert various raw values to a float when conversion is possible.
    
    Accepts numeric types (int, float), numeric strings (with optional surrounding whitespace), or None. Empty or non-numeric strings and unsupported types produce None.
    
    Parameters:
    	raw (object): Input value to convert; may be None, int, float, or str.
    
    Returns:
    	float_value (float | None): `float` if the input could be parsed or cast to a float, `None` otherwise.
    """
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
    """
    Convert a raw monetary or bonus value stored in minor units to a major-unit float.
    
    Returns:
        float: Value in major units (minor units divided by 100 using integer division), or `None` if the input is not a valid numeric value.
    """
    value = _to_optional_float(raw)
    if value is None:
        return None
    # Old DB stores monetary/bonus values in minor units; divide by 100 without remainder.
    return float(int(value) // 100)


def _to_optional_int(raw: object) -> int | None:
    """
    Convert a raw value to an integer when a clear integer representation exists.
    
    Accepts ints (returned unchanged), floats (converted by truncation toward zero), and numeric strings (whitespace-trimmed before parsing). Returns None for None, empty strings, or non-integer/non-numeric values.
    
    Returns:
        int | None: The converted integer, or `None` if conversion is not possible.
    """
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
    """
    Return the first non-None value from `row` for the given keys in order.
    
    Parameters:
        row (Mapping | Any): Mapping-like object supporting `in` and `get` (e.g., DB row or dict).
        *keys (str): Candidate keys checked in the given order.
    
    Returns:
        object: The first non-None value found for the provided keys, or `None` if none are present or all values are `None`.
    """
    for key in keys:
        if key in row:
            value = row.get(key)
            if value is not None:
                return value
    return None


def _normalize_phone_last10(phone: str | None) -> str | None:
    """
    Normalize a phone string to its last 10 digits suitable for matching.
    
    Parameters:
        phone (str | None): Input phone string which may contain non-digit characters.
    
    Returns:
        str | None: The last 10 digits of the phone if the input contains at least 10 digits, `None` otherwise.
    """
    if phone is None:
        return None
    digits = "".join(ch for ch in phone if ch.isdigit())
    if len(digits) < 10:
        return None
    return digits[-10:]
