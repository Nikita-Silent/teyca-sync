from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from app.workers.legacy_snapshot_importer import (
    ImportStats,
    LegacySnapshotImporter,
    _extract_list_ids_text,
    _is_confirmed,
    _normalize_status,
    _to_aware_utc,
    _to_optional_bool,
    _to_optional_float,
    _to_optional_int,
    _to_optional_json_object,
)


def test_extract_list_ids_text() -> None:
    assert _extract_list_ids_text(None) is None
    assert _extract_list_ids_text("") is None
    assert _extract_list_ids_text("list=7,status=confirmed") == "7"
    assert _extract_list_ids_text("1:confirmed, 2:unconfirmed, 1:blocked") == "1,2"


def test_is_confirmed() -> None:
    assert _is_confirmed(status="confirmed", bonus_updated=False) is True
    assert _is_confirmed(status="active", bonus_updated=False) is True
    assert _is_confirmed(status="blocked", bonus_updated=False) is False
    assert _is_confirmed(status=None, bonus_updated=True) is True
    assert _is_confirmed(status=None, bonus_updated=False) is False


def test_normalize_status() -> None:
    assert _normalize_status(None) is None
    assert _normalize_status("  CONFIRMED  ") == "confirmed"
    assert _normalize_status("") is None


def test_optional_numeric_parsers() -> None:
    assert _to_optional_int("42") == 42
    assert _to_optional_int("bad") is None
    assert _to_optional_float("10.5") == 10.5
    assert _to_optional_float("10,5") == 10.5
    assert _to_optional_float("bad") is None


def test_optional_json_object_parser() -> None:
    assert _to_optional_json_object({"values": [892]}) == {"values": [892]}
    assert _to_optional_json_object('{"values":[893]}') == {"values": [893]}
    assert _to_optional_json_object('["bad"]') is None
    assert _to_optional_json_object("bad") is None


def test_optional_bool_and_datetime_parsers() -> None:
    assert _to_optional_bool(True) is True
    assert _to_optional_bool("yes") is True
    assert _to_optional_bool("0") is False
    assert _to_optional_bool("bad") is None

    aware = datetime(2026, 3, 10, 12, 0, tzinfo=UTC)
    assert _to_aware_utc(aware) == aware

    naive = datetime(2026, 3, 10, 12, 0)
    assert _to_aware_utc(naive) == naive.replace(tzinfo=UTC)


@pytest.mark.asyncio
async def test_run_uses_rollback_in_dry_run() -> None:
    session = AsyncMock()
    source_conn = AsyncMock()
    source_engine = AsyncMock()
    source_engine.connect = AsyncMock(return_value=source_conn)

    with patch(
        "app.workers.legacy_snapshot_importer.create_async_engine", return_value=source_engine
    ):
        importer = LegacySnapshotImporter(
            source_db_url="postgresql+asyncpg://legacy", session=session
        )
        importer._ensure_target_is_empty = AsyncMock()
        importer._import_users = AsyncMock(return_value=(10, 0))
        importer._import_listmonk_users = AsyncMock(return_value=(7, 0))
        importer._import_merge_log = AsyncMock(return_value=(5, 0))
        importer._import_consent_accrual_log = AsyncMock(return_value=(3, 0))

        stats = await importer.run(dry_run=True)

    assert stats == ImportStats(
        users_imported=10,
        users_skipped=0,
        listmonk_users_imported=7,
        listmonk_users_skipped=0,
        merge_rows_imported=5,
        merge_rows_skipped=0,
        consent_accrual_rows_imported=3,
        consent_accrual_rows_skipped=0,
        field_stats={},
    )
    session.rollback.assert_awaited_once()
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_uses_commit_without_dry_run() -> None:
    session = AsyncMock()
    source_conn = AsyncMock()
    source_engine = AsyncMock()
    source_engine.connect = AsyncMock(return_value=source_conn)

    with patch(
        "app.workers.legacy_snapshot_importer.create_async_engine", return_value=source_engine
    ):
        importer = LegacySnapshotImporter(
            source_db_url="postgresql+asyncpg://legacy", session=session
        )
        importer._ensure_target_is_empty = AsyncMock()
        importer._import_users = AsyncMock(return_value=(1, 0))
        importer._import_listmonk_users = AsyncMock(return_value=(1, 0))
        importer._import_merge_log = AsyncMock(return_value=(1, 0))
        importer._import_consent_accrual_log = AsyncMock(return_value=(1, 0))

        await importer.run(dry_run=False)

    session.commit.assert_awaited_once()
    session.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_import_users_maps_referal_and_tags() -> None:
    session = AsyncMock()
    importer = LegacySnapshotImporter(source_db_url="postgresql+asyncpg://legacy", session=session)
    importer._insert_rows_in_chunks = AsyncMock()

    source_conn = AsyncMock()
    source_conn.execute.return_value = SimpleNamespace(
        mappings=lambda: SimpleNamespace(
            all=lambda: [
                {
                    "user_id": 1,
                    "email": "user@example.com",
                    "phone": "79039859055",
                    "first_name": "Ivan",
                    "last_name": "Ivanov",
                    "pat_name": None,
                    "birthday": None,
                    "gender": None,
                    "barcode": None,
                    "discount": None,
                    "bonus": "10",
                    "loyalty_level": None,
                    "summ": "20",
                    "summ_all": "30",
                    "summ_last": "40",
                    "visits": "2",
                    "visits_all": "3",
                    "date_last": None,
                    "city": "Novokuznetsk",
                    "referal": "4243447",
                    "tags": {"values": [892]},
                    "created_at": None,
                    "updated_at": None,
                }
            ]
        )
    )

    imported, skipped = await importer._import_users(source_conn=source_conn)

    assert imported == 1
    assert skipped == 0
    importer._insert_rows_in_chunks.assert_awaited_once()
    rows = importer._insert_rows_in_chunks.await_args.kwargs["rows"]
    assert rows[0]["referal"] == "4243447"
    assert rows[0]["tags"] == {"values": [892]}
