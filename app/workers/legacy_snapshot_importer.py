"""One-off import of legacy DB snapshot into current master DB."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
import re
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
        """
        Initialize the importer with a legacy source database URL, a target DB session, and an insertion batch size.
        
        Validates inputs, prepares internal state, and stores the provided target session and batch size for subsequent import operations.
        
        Parameters:
            source_db_url (str): Connection URL for the legacy PostgreSQL source database; must be a non-empty string.
            session (AsyncSession): Async SQLAlchemy session used for target database operations.
            batch_size (int): Number of rows to insert per chunk; must be greater than or equal to 1.
        
        Raises:
            LegacySnapshotImportError: If `source_db_url` is empty or `batch_size` is less than 1.
        """
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
        """
        Execute the full legacy snapshot import into the target database.
        
        This runs the complete import sequence (users, listmonk users, merge log, consent accrual log)
        inside a single transaction on the target DB, then commits the transaction on success or rolls
        it back on failure.
        
        Parameters:
            dry_run (bool): If True, perform the import but roll back instead of committing changes.
        
        Returns:
            ImportStats: Aggregated counts of imported and skipped rows and per-field parsing statistics.
        """
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

        stats.field_stats = {field: dict(counter) for field, counter in sorted(self._field_stats.items())}
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
        """
        Ensure the target database tables required for import are empty.
        
        Checks the User, ListmonkUser, MergeLog, and BonusAccrualLog tables and raises an error if any contain rows.
        
        Raises:
            LegacySnapshotImportError: if any of the checked tables contains one or more rows; the exception message includes per-table counts.
        """
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
        """
        Execute the given SQLAlchemy select statement and return its scalar result as an integer.
        
        Parameters:
        	stmt (Select[Any]): A SQLAlchemy Select that yields a single numeric scalar (for example, a COUNT()).
        
        Returns:
        	int: The scalar result converted to int, or 0 if the query returned None.
        """
        value = await self._session.scalar(stmt)
        return int(value or 0)

    async def _import_users(self, *, source_conn: Any) -> tuple[int, int]:
        """
        Import users from the legacy loyalty_users table into the target User model.
        
        Reads rows from the source connection, parses and normalizes fields into User payloads, inserts the payloads in configured batch sizes, and counts rows skipped due to invalid user identifiers.
        
        Returns:
            imported (int): Number of user records inserted into the target User table.
            skipped (int): Number of source rows skipped because the source user_id was invalid or missing.
        """
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
                    "loyalty_level": self._parse_str(row.get("loyalty_level"), field="users.loyalty_level"),
                    "summ": self._parse_float(row.get("summ"), field="users.summ"),
                    "summ_all": self._parse_float(row.get("summ_all"), field="users.summ_all"),
                    "summ_last": self._parse_float(row.get("summ_last"), field="users.summ_last"),
                    "check_summ": None,
                    "visits": self._parse_int(row.get("visits"), field="users.visits"),
                    "visits_all": self._parse_int(row.get("visits_all"), field="users.visits_all"),
                    "date_last": self._parse_str(row.get("date_last"), field="users.date_last"),
                    "city": self._parse_str(row.get("city"), field="users.city"),
                    "created_at": self._parse_datetime(row.get("created_at"), field="users.created_at"),
                    "updated_at": self._parse_datetime(row.get("updated_at"), field="users.updated_at"),
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
        """
        Import listmonk user records from the legacy source, transform them into ListmonkUser payloads, and bulk-insert them into the target database.
        
        Parameters:
            imported_at (datetime): Timestamp to use as a fallback for consent checked/confirmed timestamps when the source row does not provide one.
        
        Returns:
            (inserted, skipped) (tuple[int, int]): `inserted` is the number of payloads inserted into ListmonkUser; `skipped` is the number of legacy rows ignored because either `user_id` or `listmonk_user_id` could not be parsed.
        """
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
            status = self._parse_status(row.get("listmonk_user_status"), field="listmonk_users.status")
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
        """
        Import merge history records from the legacy source database into the MergeLog table.
        
        Returns:
            tuple[int, int]: (inserted, skipped) where `inserted` is the number of rows written to MergeLog
            and `skipped` is the number of source rows ignored because the `user_id` was invalid.
        """
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
        """
        Import eligible legacy listmonk user rows as email-consent entries in BonusAccrualLog.
        
        Processes rows from the legacy `listmonk_users` table, filters for confirmed consent (or legacy bonus-updated flags), builds idempotent bonus-accrual records (using an idempotency key based on user_id), and inserts them into BonusAccrualLog using on-conflict-do-nothing to avoid duplicates.
        
        Parameters:
            source_conn (Any): Active connection to the legacy source database used to execute the SELECT query.
            imported_at (datetime): Fallback timestamp used when a source row lacks an updated_at; assigned to created_at/updated_at/processed_at.
        
        Returns:
            tuple[int, int]: (inserted, skipped) where `inserted` is the number of rows written to BonusAccrualLog and `skipped` is the number of source rows ignored due to missing or unconfirmed data.
        """
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

            updated_at = self._parse_datetime(row.get("updated_at"), field="bonus_accrual_log.updated_at")
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
            stmt = insert(BonusAccrualLog).values(chunk).on_conflict_do_nothing(
                constraint="uq_bonus_accrual_idempotency_key"
            )
            result_target = await self._session.execute(stmt)
            inserted += int(result_target.rowcount or 0)
        return inserted, skipped

    async def _insert_rows_in_chunks(self, *, model: Any, rows: list[dict[str, Any]]) -> None:
        """
        Insert the provided row payloads into the specified SQLAlchemy model in batch-sized chunks.
        
        Parameters:
            model (Any): The SQLAlchemy table or mapped model to insert into.
            rows (list[dict[str, Any]]): List of dictionaries where each dict maps column names to values for a single row.
        
        """
        for chunk in _chunked(rows, self._batch_size):
            stmt = insert(model).values(chunk)
            await self._session.execute(stmt)

    def _track(self, *, field: str, status: str) -> None:
        """
        Record an occurrence of a parsing/validation outcome for a specific field.
        
        Parameters:
            field (str): The field name being tracked (e.g., "email", "bonus").
            status (str): The outcome category to count (e.g., "parsed", "null", "invalid").
        """
        self._field_stats[field][status] += 1

    def _parse_str(self, raw: object, *, field: str) -> str | None:
        """
        Parse an arbitrary value into a trimmed string and record the parse status for a named field.
        
        Parameters:
            raw (object): The input value to convert; may be any type (e.g., str, bytes, None).
            field (str): The name of the field used when recording parsing statistics.
        
        Returns:
            str | None: The trimmed string when the input contains non-empty text, otherwise `None`.
        """
        value = _to_optional_str(raw)
        self._track(field=field, status="parsed" if value is not None else "null")
        return value

    def _parse_int(self, raw: object, *, field: str) -> int | None:
        """
        Parse an input value into an integer and record the parsing outcome for the given field.
        
        Records one of three tracking statuses for the field: "parsed" when conversion succeeds, "invalid" when input is non-empty but cannot be converted, and "null" when input is empty or None.
        
        Parameters:
            raw (object): The raw value to convert to an int.
            field (str): The name of the field used when recording parsing statistics.
        
        Returns:
            int | None: The parsed integer if conversion succeeded, otherwise `None`.
        """
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
        """
        Parse a raw value into a float and record field-level parse status.
        
        Parameters:
        	raw (object): The raw input to parse; may be int, float, Decimal, or string.
        	field (str): Field name used when recording parse statistics.
        
        Returns:
        	float_value (float | None): The parsed float when successful, `None` if the input is empty or cannot be parsed.
        """
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
        """
        Parse a raw value into a boolean and record the parse status for the given field.
        
        Parameters:
        	raw: The raw input to interpret as a boolean (any type).
        	field (str): Field name used when recording parse status.
        
        Returns:
        	True if the input represents a truthy boolean, False otherwise. When the input cannot be interpreted as a boolean or is None, records a "null" status and returns False; otherwise records "parsed".
        """
        value = _to_optional_bool(raw)
        if value is None:
            self._track(field=field, status="null")
            return False
        self._track(field=field, status="parsed")
        return value

    def _parse_datetime(self, raw: object, *, field: str) -> datetime | None:
        """
        Parse a raw value into a timezone-aware UTC datetime.
        
        Parameters:
            raw (object): Value to parse (datetime-like or ISO-formatted string); may be None or empty.
            field (str): Name of the source field used when recording parse statistics.
        
        Returns:
            datetime | None: A UTC-aware datetime if parsing succeeds, `None` if the input is empty or invalid.
        
        Notes:
            Records the parse outcome in the importer's field statistics as "parsed", "invalid", or "null".
        """
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
        """
        Normalize and validate a status value and record per-field parsing statistics.
        
        Parameters:
        	raw (object): Raw input value containing the status (string or other). May be None or empty.
        	field (str): Field name used when recording parsing status into internal tracking.
        
        Returns:
        	str | None: Normalized status string (lowercased and trimmed) when valid, otherwise `None`. 
        
        Notes:
        	Updates internal field statistics with one of "parsed", "invalid", or "null" depending on the input.
        """
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
        """
        Parse a raw text field containing list IDs and return a normalized comma-separated string of sorted numeric IDs.
        
        Parameters:
        	raw (str | None): Raw input text that may contain numeric list IDs.
        	field (str): Name of the source field used for recording parse status in internal statistics.
        
        Returns:
        	str | None: Comma-separated numeric IDs sorted in ascending order (e.g. "1,2,42") if any IDs were found and parsed; `None` if the input is empty or no valid IDs were extracted.
        
        Notes:
        	Records a per-field parse status ('parsed', 'invalid', or 'null') via the importer's tracking mechanism.
        """
        value = _extract_list_ids_text(raw)
        if value is not None:
            self._track(field=field, status="parsed")
            return value
        if _has_non_empty_input(raw):
            self._track(field=field, status="invalid")
        else:
            self._track(field=field, status="null")
        return None


def _to_optional_str(raw: object) -> str | None:
    """
    Convert a value to a trimmed string or return None when there is no meaningful text.
    
    Parameters:
        raw (object): Input value to convert; if `None` or its string form is empty after trimming, it is treated as absent.
    
    Returns:
        str | None: Trimmed string representation of `raw` if non-empty, otherwise `None`.
    """
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _to_optional_int(raw: object) -> int | None:
    """
    Convert various raw values into an integer when possible.
    
    Parameters:
    	raw (object): Input value to parse. Accepts ints, floats, Decimal instances, and strings containing an integer representation (whitespace is trimmed). None, empty strings, and non-numeric strings are treated as missing.
    
    Returns:
    	int_value (int | None): The parsed integer, or None if the input is None, empty, or cannot be converted to an integer.
    """
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
    """
    Convert a raw value to a float if it represents a numeric value, otherwise return None.
    
    Parameters:
    	raw (object): Value to convert. Accepts int, float, Decimal, or numeric strings (commas allowed as decimal separators). Empty or non-numeric inputs return None.
    
    Returns:
    	float | None: The converted float value, or None if conversion is not possible.
    """
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
        except (InvalidOperation, ValueError):
            return None
    return None


def _to_optional_bool(raw: object) -> bool | None:
    """
    Normalize common boolean-like inputs into a truth value or None.
    
    Parameters:
        raw (object): Value to interpret; accepts booleans, integers, and strings (e.g., "true", "false", "1", "0", "yes", "no"), or None.
    
    Returns:
        True if the input represents a truthy value, False if it represents a falsy value, None if the input is None, empty, or not a recognized boolean representation.
    """
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
    """
    Convert a datetime-like value to a timezone-aware UTC datetime.
    
    Parameters:
        raw (object): A datetime, an ISO-8601 datetime string, or None. For strings, leading/trailing whitespace is ignored.
    
    Returns:
        datetime | None: A datetime object with UTC tzinfo. If `raw` is a naive datetime or a datetime string without timezone, it is assumed to be in UTC. Returns `None` when `raw` is None, an empty/whitespace string, an unparsable string, or an unsupported type.
    """
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
    """
    Determine whether a value represents non-empty input.
    
    Treats None as empty; for strings returns True only if the string contains non-whitespace characters; all other non-None values are considered non-empty.
    
    Parameters:
        raw (object): The value to evaluate.
    
    Returns:
        True if the value is non-empty, False otherwise.
    """
    if raw is None:
        return False
    if isinstance(raw, str):
        return bool(raw.strip())
    return True


def _chunked(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    """
    Split a list of dictionaries into consecutive chunks of the specified maximum size.
    
    Parameters:
        items (list[dict[str, Any]]): Input list to partition.
        size (int): Maximum number of items per chunk; must be greater than zero.
    
    Returns:
        list[list[dict[str, Any]]]: A list where each element is a consecutive chunk from `items`. The final chunk may contain fewer than `size` elements.
    """
    return [items[index : index + size] for index in range(0, len(items), size)]


def _extract_list_ids_text(raw: str | None) -> str | None:
    """
    Return a sorted, deduplicated comma-separated string of integer IDs extracted from the input text.
    
    If `raw` is None or contains no digit sequences, returns None.
    
    Parameters:
        raw (str | None): Input text that may contain numeric IDs.
    
    Returns:
        str | None: Comma-separated ascending list of unique integer IDs (e.g. "1,2,3"), or `None` if no IDs are found or input is `None`.
    """
    if raw is None:
        return None
    found = sorted({int(item) for item in re.findall(r"\d+", raw)})
    if not found:
        return None
    return ",".join(str(item) for item in found)


def _normalize_status(raw: object) -> str | None:
    """
    Normalize a status-like value by trimming whitespace and converting to lowercase.
    
    Returns:
    	normalized (str | None): The trimmed, lowercased string if input contained text; `None` if input is None or empty.
    """
    value = _to_optional_str(raw)
    if value is None:
        return None
    return value.strip().lower()


def _is_confirmed(*, status: str | None, bonus_updated: bool) -> bool:
    """
    Determine whether a user's email consent should be treated as confirmed.
    
    Parameters:
    	status (str | None): Normalized user/list status (e.g., "confirmed", "active"); may be None.
    	bonus_updated (bool): Legacy flag indicating the bonus/consent was updated; when True, consent is considered confirmed regardless of `status`.
    
    Returns:
    	`true` if consent is confirmed (either `bonus_updated` is True or `status` is one of the confirmed statuses), `false` otherwise.
    """
    if bonus_updated:
        return True
    if status is None:
        return False
    return status in _CONFIRMED_STATUSES
