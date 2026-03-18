from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from app.repositories.old_db import (
    OldDBRepository,
    OldDBRepositoryError,
    OldUserData,
    _normalize_phone_last10,
    _pick_first,
    _to_optional_float,
    _to_optional_int,
)


def test_old_user_data_has_merge_data() -> None:
    assert OldUserData().has_merge_data() is False
    assert OldUserData(summ=0, visits=0).has_merge_data() is False
    assert OldUserData(summ=1).has_merge_data() is True


def test_old_db_helpers() -> None:
    assert _to_optional_float("1.5") == 1.5
    assert _to_optional_float("bad") is None
    assert _to_optional_float(" ") is None
    assert _to_optional_float(object()) is None
    assert _to_optional_int("3") == 3
    assert _to_optional_int(3) == 3
    assert _to_optional_int(3.4) == 3
    assert _to_optional_int(" ") is None
    assert _to_optional_int("bad") is None
    assert _to_optional_int(object()) is None
    assert _pick_first({"a": None, "b": 2}, "a", "b") == 2
    assert _pick_first({"a": None}, "a", "b") is None
    assert _normalize_phone_last10(None) is None


@pytest.mark.asyncio
async def test_get_user_data_early_returns() -> None:
    repo = OldDBRepository("  ")
    assert await repo.get_user_data(phone="79039859055") is None

    repo = OldDBRepository("postgresql://x")
    repo._columns_cache = {"summ"}
    repo._engine = AsyncMock()
    assert await repo.get_user_data(phone="79039859055") is None

    repo._columns_cache = {"phone"}
    assert await repo.get_user_data(phone="79039859055") is None

    repo._columns_cache = {"phone", "summ"}
    assert await repo.get_user_data(phone="abc") is None


@pytest.mark.asyncio
async def test_get_user_data_returns_row_and_handles_errors() -> None:
    repo = OldDBRepository("postgresql://x")
    repo._columns_cache = {"phone", "balance", "summ", "check_sum", "check_count"}

    row = {
        "balance": 2330,
        "summ": "199",
        "check_sum": "100.9",
        "visits": "1",
        "check_count": "4",
    }
    result_obj = SimpleNamespace(mappings=lambda: SimpleNamespace(first=lambda: row))
    conn = AsyncMock()
    conn.execute.return_value = result_obj
    cm = AsyncMock()
    cm.__aenter__.return_value = conn
    cm.__aexit__.return_value = False
    engine = MagicMock()
    engine.connect.return_value = cm
    engine.dispose = AsyncMock()
    repo._engine = engine

    data = await repo.get_user_data(phone="+7 (903) 985-90-55")

    assert data is not None
    assert data.bonus == 23.0
    assert data.summ == 199.0
    assert data.check_summ == 100.9
    assert data.visits == 4

    conn.execute.side_effect = SQLAlchemyError("boom")
    with pytest.raises(OldDBRepositoryError):
        await repo.get_user_data(phone="79039859055")

    conn.execute.side_effect = None
    conn.execute.return_value = SimpleNamespace(
        mappings=lambda: SimpleNamespace(first=lambda: None)
    )
    assert await repo.get_user_data(phone="79039859055") is None


@pytest.mark.asyncio
async def test_get_user_data_times_out() -> None:
    repo = OldDBRepository("postgresql://x", request_timeout_seconds=0.2)
    repo._columns_cache = {"phone", "summ"}

    conn = AsyncMock()

    async def slow_execute(*args: object, **kwargs: object) -> object:
        await asyncio.sleep(1)
        return SimpleNamespace(mappings=lambda: SimpleNamespace(first=lambda: None))

    conn.execute.side_effect = slow_execute
    cm = AsyncMock()
    cm.__aenter__.return_value = conn
    cm.__aexit__.return_value = False
    engine = MagicMock()
    engine.connect.return_value = cm
    repo._engine = engine

    with pytest.raises(OldDBRepositoryError, match="Old DB query timeout"):
        await repo.get_user_data(phone="79039859055")


@pytest.mark.asyncio
async def test_get_users_columns_and_close() -> None:
    repo = OldDBRepository("postgresql://x")
    assert await repo._get_users_columns() == set()

    conn = AsyncMock()
    conn.execute.return_value = SimpleNamespace(fetchall=lambda: [("phone",), ("summ",)])
    cm = AsyncMock()
    cm.__aenter__.return_value = conn
    cm.__aexit__.return_value = False
    engine = MagicMock()
    engine.connect.return_value = cm
    engine.dispose = AsyncMock()
    repo._engine = engine
    repo._columns_cache = None

    cols = await repo._get_users_columns()
    assert cols == {"phone", "summ"}

    # cache hit
    assert await repo._get_users_columns() == {"phone", "summ"}

    repo._columns_cache = None
    conn.execute.side_effect = SQLAlchemyError("x")
    assert await repo._get_users_columns() == set()

    await repo.close()
    engine.dispose.assert_awaited_once()
    assert repo._engine is None
    await repo.close()


@pytest.mark.asyncio
async def test_get_user_data_creates_engine_on_first_call() -> None:
    repo = OldDBRepository("postgresql://x")

    fake_engine = MagicMock()
    conn = AsyncMock()
    conn.execute.return_value = SimpleNamespace(fetchall=lambda: [("phone",), ("summ",)])
    cm = AsyncMock()
    cm.__aenter__.return_value = conn
    cm.__aexit__.return_value = False
    fake_engine.connect.return_value = cm

    with patch(
        "app.repositories.old_db.create_async_engine", return_value=fake_engine
    ) as engine_factory:
        assert await repo.get_user_data(phone="123") is None

    _, kwargs = engine_factory.call_args
    assert kwargs["connect_args"]["timeout"] == pytest.approx(15.0)
    assert kwargs["connect_args"]["command_timeout"] == pytest.approx(15.0)
