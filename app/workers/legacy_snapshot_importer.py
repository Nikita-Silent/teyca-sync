"""One-off import of legacy DB snapshot into current master DB."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import structlog
from sqlalchemy import Select, func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from app.db.models import BonusAccrualLog, ListmonkUser, MergeLog, User

logger = structlog.get_logger()

BONUS_REASON_EMAIL_CONSENT = "email_consent"
_CONFIRMED_STATUSES: set[str] = {"confirmed", "active"}
_BLOCKED_STATUSES: set[str] = {"blocked", "blocklisted", "blacklisted"}


@dataclass(slots=True)
class ImportStats:
    users_imported: int = 0
    users_skipped: int = 0
    listmonk_users_imported: int = 0
    listmonk_users_skipped: int = 0
    merge_rows_imported: int = 0
    merge_rows_skipped: int = 0
    consent_accrual_rows_imported: int = 0
    consent_accrual_rows_skipped: int = 0
    field_stats: dict[str, dict[str, int]] | None = None


class LegacySnapshotImportError(Exception):
    """Raised when legacy snapshot import cannot proceed."""


class LegacySnapshotImporter:
    """Imports users/listmonk/merge data from third-party legacy PostgreSQL DB."""

    def __init__(self, *, source_db_url: str, session: AsyncSession, batch_size: int = 500) -> None:
        normalized_url = source_db_url.strip()
        if not normalized_url:
            raise LegacySnapshotImportError("Source DB URL is empty")
        if batch_size < 1:
            raise LegacySnapshotImportError("Batch size must be >= 1")
        self._source_engine: AsyncEngine = create_async_engine(normalized_url, pool_pre_ping=True)
        self._session = session
        self._batch_size = batch_size
        self._field_stats: dict[str, Counter[str]] = defaultdict(Counter)

    async def run(self, *, dry_run: bool = False) -> ImportStats:
        """Run full one-off import in a single transaction on target DB."""
        await self._ensure_target_is_empty()
        stats = ImportStats()
        now = datetime.now(UTC)

        source_conn = await self._source_engine.connect()
        try:
            users_imported, users_skipped = await self._import_users(source_conn=source_conn)
            stats.users_imported = users_imported
            stats.users_skipped = users_skipped
            listmonk_imported, listmonk_skipped = await self._import_listmonk_users(
                source_conn=source_conn,
                imported_at=now,
            )
            stats.listmonk_users_imported = listmonk_imported
            stats.listmonk_users_skipped = listmonk_skipped
            merge_imported, merge_skipped = await self._import_merge_log(source_conn=source_conn)
            stats.merge_rows_imported = merge_imported
            stats.merge_rows_skipped = merge_skipped
            accrual_imported, accrual_skipped = await self._import_consent_accrual_log(
                source_conn=source_conn,
                imported_at=now,
            )
            stats.consent_accrual_rows_imported = accrual_imported
            stats.consent_accrual_rows_skipped = accrual_skipped
            if dry_run:
                await self._session.rollback()
            else:
                await self._session.commit()
        except Exception:
            await self._session.rollback()
            raise
        finally:
            await source_conn.close()
            await self._source_engine.dispose()

        stats.field_stats = {
            field: dict(counter) for field, counter in sorted(self._field_stats.items())
        }
        logger.info(
            "legacy_snapshot_import_done",
            dry_run=dry_run,
            users_imported=stats.users_imported,
            users_skipped=stats.users_skipped,
            listmonk_users_imported=stats.listmonk_users_imported,
            listmonk_users_skipped=stats.listmonk_users_skipped,
            merge_rows_imported=stats.merge_rows_imported,
            merge_rows_skipped=stats.merge_rows_skipped,
            consent_accrual_rows_imported=stats.consent_accrual_rows_imported,
            consent_accrual_rows_skipped=stats.consent_accrual_rows_skipped,
            field_stats=stats.field_stats,
        )
        return stats

    async def _ensure_target_is_empty(self) -> None:
        users_count = await self._count_rows(select(func.count()).select_from(User))
        listmonk_count = await self._count_rows(select(func.count()).select_from(ListmonkUser))
        merge_count = await self._count_rows(select(func.count()).select_from(MergeLog))
        accrual_count = await self._count_rows(select(func.count()).select_from(BonusAccrualLog))
        if users_count or listmonk_count or merge_count or accrual_count:
            raise LegacySnapshotImportError(
                "Target DB is not empty. "
                f"counts: users={users_count}, listmonk_users={listmonk_count}, "
                f"merge_log={merge_count}, bonus_accrual_log={accrual_count}"
            )

    async def _count_rows(self, stmt: Select[Any]) -> int:
        value = await self._session.scalar(stmt)
        return int(value or 0)

    async def _import_users(self, *, source_conn: Any) -> tuple[int, int]:
        query = text(
            """
            SELECT
                user_id,
                email,
                phone,
                first_name,
                last_name,
                pat_name,
                birthday,
                gender,
                barcode,
                discount,
                bonus,
                loyalty_level,
                summ,
                summ_all,
                summ_last,
                visits,
                visits_all,
                date_last,
                city,
                referal,
                tags,
                created_at,
                updated_at
            FROM loyalty_users
            ORDER BY user_id
            """
        )
        result = await source_conn.execute(query)
        rows = result.mappings().all()

        payload: list[dict[str, Any]] = []
        skipped = 0
        for row in rows:
            user_id = self._parse_int(row.get("user_id"), field="users.user_id")
            if user_id is None:
                skipped += 1
                continue
            payload.append(
                {
                    "user_id": user_id,
                    "email": self._parse_str(row.get("email"), field="users.email"),
                    "phone": self._parse_str(row.get("phone"), field="users.phone"),
                    "first_name": self._parse_str(row.get("first_name"), field="users.first_name"),
                    "last_name": self._parse_str(row.get("last_name"), field="users.last_name"),
                    "pat_name": self._parse_str(row.get("pat_name"), field="users.pat_name"),
                    "birthday": self._parse_str(row.get("birthday"), field="users.birthday"),
                    "gender": self._parse_str(row.get("gender"), field="users.gender"),
                    "barcode": self._parse_str(row.get("barcode"), field="users.barcode"),
                    "discount": self._parse_str(row.get("discount"), field="users.discount"),
                    "bonus": self._parse_float(row.get("bonus"), field="users.bonus"),
                    "loyalty_level": self._parse_str(
                        row.get("loyalty_level"), field="users.loyalty_level"
                    ),
                    "summ": self._parse_float(row.get("summ"), field="users.summ"),
                    "summ_all": self._parse_float(row.get("summ_all"), field="users.summ_all"),
                    "summ_last": self._parse_float(row.get("summ_last"), field="users.summ_last"),
                    "check_summ": None,
                    "visits": self._parse_int(row.get("visits"), field="users.visits"),
                    "visits_all": self._parse_int(row.get("visits_all"), field="users.visits_all"),
                    "date_last": self._parse_str(row.get("date_last"), field="users.date_last"),
                    "city": self._parse_str(row.get("city"), field="users.city"),
                    "referal": self._parse_str(row.get("referal"), field="users.referal"),
                    "tags": self._parse_json_object(row.get("tags"), field="users.tags"),
                    "created_at": self._parse_datetime(
                        row.get("created_at"), field="users.created_at"
                    ),
                    "updated_at": self._parse_datetime(
                        row.get("updated_at"), field="users.updated_at"
                    ),
                }
            )

        if not payload:
            return 0, skipped
        await self._insert_rows_in_chunks(model=User, rows=payload)
        return len(payload), skipped

    async def _import_listmonk_users(
        self,
        *,
        source_conn: Any,
        imported_at: datetime,
    ) -> tuple[int, int]:
        query = text(
            """
            SELECT
                lm.user_id,
                lm.listmonk_user_id,
                lm.listmonk_user_status,
                lm.listmonk_list_id_status,
                lm.bonus_updated,
                lm.created_at,
                lm.updated_at,
                lu.email
            FROM listmonk_users lm
            LEFT JOIN loyalty_users lu ON lu.user_id = lm.user_id
            ORDER BY lm.user_id
            """
        )
        result = await source_conn.execute(query)
        rows = result.mappings().all()
        payload: list[dict[str, Any]] = []
        skipped = 0

        for row in rows:
            user_id = self._parse_int(row.get("user_id"), field="listmonk_users.user_id")
            subscriber_id = self._parse_int(
                row.get("listmonk_user_id"),
                field="listmonk_users.subscriber_id",
            )
            if user_id is None or subscriber_id is None:
                skipped += 1
                continue
            status = self._parse_status(
                row.get("listmonk_user_status"), field="listmonk_users.status"
            )
            list_status_raw = self._parse_str(
                row.get("listmonk_list_id_status"),
                field="listmonk_users.legacy_list_id_status",
            )
            bonus_updated = self._parse_bool(
                row.get("bonus_updated"),
                field="listmonk_users.legacy_bonus_updated",
            )
            confirmed = _is_confirmed(status=status, bonus_updated=bonus_updated)

            payload.append(
                {
                    "user_id": user_id,
                    "subscriber_id": subscriber_id,
                    "email": self._parse_str(row.get("email"), field="listmonk_users.email"),
                    "status": status,
                    "list_ids": self._parse_list_ids_text(
                        list_status_raw,
                        field="listmonk_users.list_ids",
                    ),
                    "attributes": {
                        "legacy_list_id_status": list_status_raw,
                        "legacy_bonus_updated": bonus_updated,
                    },
                    "consent_pending": not confirmed and status not in _BLOCKED_STATUSES,
                    "consent_checked_at": self._parse_datetime(
                        row.get("updated_at"),
                        field="listmonk_users.updated_at_for_checked",
                    )
                    or imported_at,
                    "consent_confirmed_at": (
                        self._parse_datetime(
                            row.get("updated_at"),
                            field="listmonk_users.updated_at_for_confirmed",
                        )
                        or imported_at
                    )
                    if confirmed
                    else None,
                    "created_at": self._parse_datetime(
                        row.get("created_at"),
                        field="listmonk_users.created_at",
                    ),
                    "updated_at": self._parse_datetime(
                        row.get("updated_at"),
                        field="listmonk_users.updated_at",
                    ),
                }
            )

        if not payload:
            return 0, skipped
        await self._insert_rows_in_chunks(model=ListmonkUser, rows=payload)
        return len(payload), skipped

    async def _import_merge_log(self, *, source_conn: Any) -> tuple[int, int]:
        query = text(
            """
            SELECT user_id, merged_at
            FROM merge_history
            ORDER BY user_id, merged_at
            """
        )
        result = await source_conn.execute(query)
        rows = result.mappings().all()
        payload: list[dict[str, Any]] = []
        skipped = 0

        for row in rows:
            user_id = self._parse_int(row.get("user_id"), field="merge_log.user_id")
            merged_at = self._parse_datetime(row.get("merged_at"), field="merge_log.merged_at")
            if user_id is None:
                skipped += 1
                continue
            payload.append(
                {
                    "user_id": user_id,
                    "merged_at": merged_at,
                    "source_event_type": "legacy_import",
                    "source_event_id": None,
                    "trace_id": None,
                    "merge_version": 1,
                }
            )

        if not payload:
            return 0, skipped
        await self._insert_rows_in_chunks(model=MergeLog, rows=payload)
        return len(payload), skipped

    async def _import_consent_accrual_log(
        self,
        *,
        source_conn: Any,
        imported_at: datetime,
    ) -> tuple[int, int]:
        query = text(
            """
            SELECT
                user_id,
                listmonk_user_id,
                listmonk_user_status,
                bonus_updated,
                updated_at
            FROM listmonk_users
            ORDER BY user_id
            """
        )
        result = await source_conn.execute(query)
        rows = result.mappings().all()
        payload: list[dict[str, Any]] = []
        skipped = 0

        for row in rows:
            user_id = self._parse_int(row.get("user_id"), field="bonus_accrual_log.user_id")
            subscriber_id = self._parse_int(
                row.get("listmonk_user_id"),
                field="bonus_accrual_log.subscriber_id",
            )
            if user_id is None:
                skipped += 1
                continue
            status = self._parse_status(
                row.get("listmonk_user_status"),
                field="bonus_accrual_log.legacy_status",
            )
            bonus_updated = self._parse_bool(
                row.get("bonus_updated"),
                field="bonus_accrual_log.legacy_bonus_updated",
            )
            if not _is_confirmed(status=status, bonus_updated=bonus_updated):
                skipped += 1
                continue

            updated_at = self._parse_datetime(
                row.get("updated_at"), field="bonus_accrual_log.updated_at"
            )
            finalized_at = updated_at or imported_at

            payload.append(
                {
                    "user_id": user_id,
                    "reason": BONUS_REASON_EMAIL_CONSENT,
                    "idempotency_key": f"{BONUS_REASON_EMAIL_CONSENT}:{user_id}",
                    "status": "done",
                    "payload": {
                        "bonus_done": True,
                        "key1_done": True,
                        "subscriber_id": subscriber_id,
                        "legacy_bonus_updated": bonus_updated,
                    },
                    "error_text": None,
                    "created_at": finalized_at,
                    "updated_at": finalized_at,
                    "processed_at": finalized_at,
                }
            )

        if not payload:
            return 0, skipped
        inserted = 0
        for chunk in _chunked(payload, self._batch_size):
            stmt = (
                insert(BonusAccrualLog)
                .values(chunk)
                .on_conflict_do_nothing(constraint="uq_bonus_accrual_idempotency_key")
            )
            result_target = await self._session.execute(stmt)
            inserted += int(result_target.rowcount or 0)
        return inserted, skipped

    async def _insert_rows_in_chunks(self, *, model: Any, rows: list[dict[str, Any]]) -> None:
        for chunk in _chunked(rows, self._batch_size):
            stmt = insert(model).values(chunk)
            await self._session.execute(stmt)

    def _track(self, *, field: str, status: str) -> None:
        self._field_stats[field][status] += 1

    def _parse_str(self, raw: object, *, field: str) -> str | None:
        value = _to_optional_str(raw)
        self._track(field=field, status="parsed" if value is not None else "null")
        return value

    def _parse_int(self, raw: object, *, field: str) -> int | None:
        value = _to_optional_int(raw)
        if value is not None:
            self._track(field=field, status="parsed")
            return value
        if _has_non_empty_input(raw):
            self._track(field=field, status="invalid")
        else:
            self._track(field=field, status="null")
        return None

    def _parse_float(self, raw: object, *, field: str) -> float | None:
        value = _to_optional_float(raw)
        if value is not None:
            self._track(field=field, status="parsed")
            return value
        if _has_non_empty_input(raw):
            self._track(field=field, status="invalid")
        else:
            self._track(field=field, status="null")
        return None

    def _parse_bool(self, raw: object, *, field: str) -> bool:
        value = _to_optional_bool(raw)
        if value is None:
            self._track(field=field, status="null")
            return False
        self._track(field=field, status="parsed")
        return value

    def _parse_datetime(self, raw: object, *, field: str) -> datetime | None:
        value = _to_aware_utc(raw)
        if value is not None:
            self._track(field=field, status="parsed")
            return value
        if _has_non_empty_input(raw):
            self._track(field=field, status="invalid")
        else:
            self._track(field=field, status="null")
        return None

    def _parse_status(self, raw: object, *, field: str) -> str | None:
        value = _normalize_status(raw)
        if value is not None:
            self._track(field=field, status="parsed")
            return value
        if _has_non_empty_input(raw):
            self._track(field=field, status="invalid")
        else:
            self._track(field=field, status="null")
        return None

    def _parse_list_ids_text(self, raw: str | None, *, field: str) -> str | None:
        value = _extract_list_ids_text(raw)
        if value is not None:
            self._track(field=field, status="parsed")
            return value
        if _has_non_empty_input(raw):
            self._track(field=field, status="invalid")
        else:
            self._track(field=field, status="null")
        return None

    def _parse_json_object(self, raw: object, *, field: str) -> dict[str, Any] | None:
        value = _to_optional_json_object(raw)
        if value is not None:
            self._track(field=field, status="parsed")
            return value
        if _has_non_empty_input(raw):
            self._track(field=field, status="invalid")
        else:
            self._track(field=field, status="null")
        return None


def _to_optional_str(raw: object) -> str | None:
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _to_optional_int(raw: object) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        return int(raw)
    if isinstance(raw, Decimal):
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


def _to_optional_float(raw: object) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, Decimal):
        return float(raw)
    if isinstance(raw, str):
        stripped = raw.strip().replace(",", ".")
        if not stripped:
            return None
        try:
            return float(Decimal(stripped))
        except InvalidOperation, ValueError:
            return None
    return None


def _to_optional_json_object(raw: object) -> dict[str, Any] | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            return None
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, dict):
            return parsed
    return None


def _to_optional_bool(raw: object) -> bool | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, int):
        return raw != 0
    if isinstance(raw, str):
        value = raw.strip().lower()
        if not value:
            return None
        if value in {"1", "true", "t", "yes", "y"}:
            return True
        if value in {"0", "false", "f", "no", "n"}:
            return False
    return None


def _to_aware_utc(raw: object) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            return raw.replace(tzinfo=UTC)
        return raw.astimezone(UTC)
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            return None
        try:
            parsed = datetime.fromisoformat(stripped)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return None


def _has_non_empty_input(raw: object) -> bool:
    if raw is None:
        return False
    if isinstance(raw, str):
        return bool(raw.strip())
    return True


def _chunked(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _extract_list_ids_text(raw: str | None) -> str | None:
    if raw is None:
        return None
    found = sorted({int(item) for item in re.findall(r"\d+", raw)})
    if not found:
        return None
    return ",".join(str(item) for item in found)


def _normalize_status(raw: object) -> str | None:
    value = _to_optional_str(raw)
    if value is None:
        return None
    return value.strip().lower()


def _is_confirmed(*, status: str | None, bonus_updated: bool) -> bool:
    if bonus_updated:
        return True
    if status is None:
        return False
    return status in _CONFIRMED_STATUSES
