"""One-time backfill: pay the consent bonus to clients missed by the broken
confirmed-status condition (С2/teyca-sync-io3).

Before this fix (teyca-sync-ge6), the bonus only reached subscribers whose
Listmonk status happened to read `confirmed`/`enabled`/`active` at poll time.
106 clients (54 `unconfirmed` + 52 `enabled`) never got paid: valid,
non-conflicting email, no `bonus_accrual_log` entry for the consent bonus.
Owner decision 2026-07-31 (see docs/reverse-engineering-plan.md, section 12):
pay them retroactively, once.

Uses the exact same idempotency key (`email_consent:{user_id}`) as the live
accrual path in `external_dispatcher_worker.py`, so this can never double-pay
a user the live path already paid, and running this script twice is a no-op
the second time. Before paying, cross-checks Teyca's own operation history
(`POST /operations`, `user_ids[]` filter) as a secondary safety net against
grants made outside this system.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import httpx
import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.teyca import BonusOperation, TeycaAPIError, TeycaClient, build_teyca_client
from app.config import Settings, get_settings
from app.consumers.common import is_valid_email
from app.db.session import SessionLocal
from app.repositories.bonus_accrual import BonusAccrualRepository
from app.repositories.listmonk_users import ListmonkUsersRepository

logger = structlog.get_logger()

BONUS_REASON_EMAIL_CONSENT = "email_consent"
TEYCA_KEY1_CONFIRMED = "confirmed"
BACKFILL_STATUSES = ("unconfirmed", "enabled")


class ConsentBonusBackfillError(RuntimeError):
    """Raised when the one-time consent bonus backfill cannot proceed safely."""


@dataclass(slots=True, frozen=True)
class ConsentBonusCandidate:
    """One user eligible for the retroactive consent bonus."""

    user_id: int
    subscriber_id: int
    email: str
    status: str


@dataclass(slots=True)
class ConsentBonusBackfillSummary:
    """Aggregated counts for one backfill run."""

    candidates: int = 0
    skipped_operations_match: int = 0
    accrued: int = 0
    failed: int = 0


def consent_bonus_idempotency_key(user_id: int) -> str:
    return f"{BONUS_REASON_EMAIL_CONSENT}:{user_id}"


@dataclass(slots=True)
class ConsentBonusBackfill:
    """One-time backfill helper for the 106 clients missed by С2."""

    settings: Settings
    session_factory: async_sessionmaker[AsyncSession]
    teyca_client: TeycaClient

    async def _run_in_session(
        self,
        operation: Callable[[AsyncSession], Awaitable[Any]],
    ) -> Any:
        """Run one short database phase in its own transaction."""
        async with self.session_factory() as session:
            try:
                result = await operation(session)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
        return result

    async def collect_candidates(self) -> list[ConsentBonusCandidate]:
        """Select unconfirmed/enabled users with a valid, non-conflicting email
        and no existing `bonus_accrual_log` entry for the consent bonus."""

        async def operation(session: AsyncSession) -> list[ConsentBonusCandidate]:
            listmonk_repo = ListmonkUsersRepository(session)
            accrual_repo = BonusAccrualRepository(session)
            duplicate_emails = set(await listmonk_repo.get_duplicate_emails())
            rows = await listmonk_repo.get_by_statuses(statuses=list(BACKFILL_STATUSES))

            candidates: list[ConsentBonusCandidate] = []
            for row in rows:
                email = row.email
                if not is_valid_email(email):
                    continue
                assert email is not None
                normalized_email = email.strip().lower()
                if normalized_email in duplicate_emails:
                    continue
                user_id = int(row.user_id)
                existing = await accrual_repo.get_by_key(
                    idempotency_key=consent_bonus_idempotency_key(user_id)
                )
                if existing is not None and existing.status == "done":
                    continue
                candidates.append(
                    ConsentBonusCandidate(
                        user_id=user_id,
                        subscriber_id=int(row.subscriber_id),
                        email=normalized_email,
                        status=str(row.status),
                    )
                )
            return candidates

        return await self._run_in_session(operation)

    async def reconcile_with_teyca(
        self, *, candidates: list[ConsentBonusCandidate]
    ) -> set[int]:
        """Return user_ids that already show a matching-value operation in Teyca
        history — these must not be paid again even without a local log entry."""
        if not candidates:
            return set()
        user_ids = [candidate.user_id for candidate in candidates]
        operations = await self.teyca_client.list_operations(
            user_ids=user_ids,
            limit=max(100, len(user_ids) * 2),
        )
        expected_value = str(self.settings.consent_bonus_amount)
        matched: set[int] = set()
        for op in operations:
            op_user_id = _to_optional_int(op.get("user_id"))
            op_value = _to_optional_str(op.get("value"))
            if op_user_id is not None and op_value == expected_value:
                matched.add(op_user_id)
        return matched

    async def accrue(
        self,
        *,
        candidates: list[ConsentBonusCandidate],
        already_paid: set[int],
    ) -> ConsentBonusBackfillSummary:
        """Pay the consent bonus to every candidate not already covered by
        `already_paid`, skipping over failures so one bad user doesn't block
        the rest of the batch."""
        summary = ConsentBonusBackfillSummary(candidates=len(candidates))
        for candidate in candidates:
            if candidate.user_id in already_paid:
                summary.skipped_operations_match += 1
                logger.warning(
                    "consent_bonus_backfill_operations_match",
                    user_id=candidate.user_id,
                )
                continue
            try:
                await self._accrue_one(user_id=candidate.user_id)
                summary.accrued += 1
                logger.info("consent_bonus_backfill_accrued", user_id=candidate.user_id)
            except (TeycaAPIError, httpx.HTTPError) as exc:
                summary.failed += 1
                logger.error(
                    "consent_bonus_backfill_failed",
                    user_id=candidate.user_id,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
        return summary

    async def _accrue_one(self, *, user_id: int) -> None:
        idempotency_key = consent_bonus_idempotency_key(user_id)
        await self._reserve(user_id=user_id, idempotency_key=idempotency_key)
        saved_payload = await self._get_payload(idempotency_key=idempotency_key)
        if saved_payload is None:
            raise ConsentBonusBackfillError(
                f"bonus_accrual_log row missing right after reserve: {idempotency_key}"
            )
        payload = {
            "bonus_done": bool(saved_payload.get("bonus_done", False)),
            "key1_done": bool(saved_payload.get("key1_done", False)),
        }

        if not payload["bonus_done"]:
            await self.teyca_client.accrue_bonuses(
                user_id=user_id,
                bonuses=[BonusOperation.one_shot(value=str(self.settings.consent_bonus_amount))],
            )
            payload["bonus_done"] = True
            await self._save_progress(idempotency_key=idempotency_key, payload=payload)

        if not payload["key1_done"]:
            await self.teyca_client.update_pass_fields(
                user_id=user_id,
                fields={"key1": TEYCA_KEY1_CONFIRMED},
            )
            payload["key1_done"] = True
            await self._save_progress(idempotency_key=idempotency_key, payload=payload)

        await self._mark_done(idempotency_key=idempotency_key, payload=payload)

    async def _reserve(self, *, user_id: int, idempotency_key: str) -> None:
        async def operation(session: AsyncSession) -> None:
            repo = BonusAccrualRepository(session)
            await repo.reserve(
                user_id=user_id,
                reason=BONUS_REASON_EMAIL_CONSENT,
                idempotency_key=idempotency_key,
                payload={"bonus_done": False, "key1_done": False},
            )

        await self._run_in_session(operation)

    async def _get_payload(self, *, idempotency_key: str) -> dict[str, Any] | None:
        async def operation(session: AsyncSession) -> dict[str, Any] | None:
            repo = BonusAccrualRepository(session)
            current = await repo.get_by_key(idempotency_key=idempotency_key)
            return None if current is None else dict(current.payload or {})

        return await self._run_in_session(operation)

    async def _save_progress(self, *, idempotency_key: str, payload: dict[str, Any]) -> None:
        async def operation(session: AsyncSession) -> None:
            repo = BonusAccrualRepository(session)
            await repo.save_progress(
                idempotency_key=idempotency_key,
                payload=payload,
                status="pending",
                error_text=None,
            )

        await self._run_in_session(operation)

    async def _mark_done(self, *, idempotency_key: str, payload: dict[str, Any]) -> None:
        async def operation(session: AsyncSession) -> None:
            repo = BonusAccrualRepository(session)
            await repo.mark_done_with_payload(idempotency_key=idempotency_key, payload=payload)

        await self._run_in_session(operation)


def build_consent_bonus_backfill() -> ConsentBonusBackfill:
    """Build backfill helper from current settings and default clients."""
    settings = get_settings()
    return ConsentBonusBackfill(
        settings=settings,
        session_factory=SessionLocal,
        teyca_client=build_teyca_client(settings, session_factory=SessionLocal),
    )


def _to_optional_int(raw: object) -> int | None:
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str) and raw.strip().lstrip("-").isdigit():
        return int(raw.strip())
    return None


def _to_optional_str(raw: object) -> str | None:
    if isinstance(raw, str):
        return raw.strip()
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return str(raw)
    return None
