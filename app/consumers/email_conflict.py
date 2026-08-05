"""Runtime resolution for the users.email race (teyca-sync-eh8).

Before the unique index (teyca-sync-4wh), two concurrent writers for
different user_ids could both pass `get_other_user_ids_by_email` (it locks
by user_id, but the race is between two *different* user_ids) and both
write the same email. Now `users.upsert` raises a real DB constraint
violation instead, and this module resolves it: same deterministic Р5/Р6
policy the y1c one-time cleanup used (teyca-sync-37z), same outbox
operation (`teyca_email_repair_sync`) its dispatcher already drains.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.policies.email_duplicate_policy import (
    EmailDuplicateCandidate,
    resolve_email_duplicate_group,
)
from app.repositories.email_repair_log import EmailRepairLogRepository
from app.repositories.external_call_outbox import (
    OUTBOX_OP_TEYCA_EMAIL_REPAIR_SYNC,
    ExternalCallOutboxRepository,
    dedupe_key_for_email_repair_sync,
)
from app.repositories.listmonk_users import ListmonkUsersRepository
from app.repositories.users import UsersRepository

logger = structlog.get_logger()

EMAIL_UNIQUE_CONSTRAINT = "uq_users_email_lower_trim"


def is_email_unique_violation(exc: IntegrityError) -> bool:
    """True when `exc` is the users.email race constraint, not some other integrity error."""
    orig = getattr(exc, "orig", None)
    constraint_name = getattr(orig, "constraint_name", None)
    if constraint_name is not None:
        return str(constraint_name) == EMAIL_UNIQUE_CONSTRAINT
    return EMAIL_UNIQUE_CONSTRAINT in str(exc)


async def resolve_users_email_conflict(
    *,
    session: AsyncSession,
    user_id: int,
    profile: dict[str, Any],
    source_event_type: str,
    source_event_id: str | None,
    trace_id: str | None,
) -> bool:
    """Resolve a live users.email race caught as a DB constraint violation.

    Rolls back the aborted transaction, resolves winner/loser via the
    Р5/Р6 policy (this event's incoming phone/date_last for `user_id`,
    current DB state for whoever else holds the email), clears every
    loser's email, logs each loser in email_repair_log, and enqueues the
    same outbox job the y1c backfill uses for Teyca sync. Retries
    `user_id`'s own upsert (email cleared if it lost). Returns True if
    `user_id` kept the email, False if it lost.
    """
    await session.rollback()

    users_repo = UsersRepository(session)
    listmonk_repo = ListmonkUsersRepository(session)
    repair_repo = EmailRepairLogRepository(session)
    outbox_repo = ExternalCallOutboxRepository(session)

    normalized_email = str(profile.get("email") or "").strip().lower()
    other_user_ids = [
        uid
        for uid in await users_repo.get_user_ids_by_email(email=normalized_email, limit=10)
        if uid != user_id
    ]
    if not other_user_ids:
        # The conflict evaporated by the time we got here (e.g. the other
        # side's email changed again) — just retry as a normal write.
        await users_repo.upsert(user_id=user_id, profile=profile)
        logger.info(
            "email_conflict_resolved_no_longer_conflicting",
            user_id=user_id,
            normalized_email=normalized_email,
        )
        return True

    candidates = [
        EmailDuplicateCandidate(
            user_id=user_id,
            phone=profile.get("phone"),
            date_last=profile.get("date_last"),
            updated_at=None,
        )
    ]
    for other_user_id in other_user_ids:
        other_row = await users_repo.get_by_user_id(user_id=other_user_id)
        candidates.append(
            EmailDuplicateCandidate(
                user_id=other_user_id,
                phone=other_row.phone if other_row is not None else None,
                date_last=other_row.date_last if other_row is not None else None,
                updated_at=other_row.updated_at if other_row is not None else None,
            )
        )

    resolution = resolve_email_duplicate_group(candidates)
    won = resolution.winner_user_id == user_id
    run_id = f"eh8-runtime:{datetime.now(UTC).isoformat()}"

    for loser_user_id in resolution.loser_user_ids:
        if loser_user_id == user_id:
            continue
        await users_repo.clear_email(user_id=loser_user_id)
        await listmonk_repo.clear_email(user_id=loser_user_id)
        await _enqueue_teyca_sync(
            repair_repo=repair_repo,
            outbox_repo=outbox_repo,
            normalized_email=normalized_email,
            loser_user_id=loser_user_id,
            winner_user_id=resolution.winner_user_id,
            mark_bad_email=resolution.mark_bad_email,
            source_event_id=source_event_id or run_id,
            trace_id=trace_id or run_id,
        )

    final_profile = dict(profile)
    if not won:
        final_profile["email"] = None
    await users_repo.upsert(user_id=user_id, profile=final_profile)

    if not won:
        await _enqueue_teyca_sync(
            repair_repo=repair_repo,
            outbox_repo=outbox_repo,
            normalized_email=normalized_email,
            loser_user_id=user_id,
            winner_user_id=resolution.winner_user_id,
            mark_bad_email=resolution.mark_bad_email,
            source_event_id=source_event_id or run_id,
            trace_id=trace_id or run_id,
        )

    logger.info(
        "email_conflict_resolved",
        user_id=user_id,
        normalized_email=normalized_email,
        winner_user_id=resolution.winner_user_id,
        loser_user_ids=resolution.loser_user_ids,
        won=won,
        mark_bad_email=resolution.mark_bad_email,
    )
    return won


async def _enqueue_teyca_sync(
    *,
    repair_repo: EmailRepairLogRepository,
    outbox_repo: ExternalCallOutboxRepository,
    normalized_email: str,
    loser_user_id: int,
    winner_user_id: int,
    mark_bad_email: bool,
    source_event_id: str | None,
    trace_id: str | None,
) -> None:
    repair_id = await repair_repo.create_db_applied(
        normalized_email=normalized_email,
        incoming_user_id=loser_user_id,
        existing_user_id=winner_user_id,
        winner_user_id=winner_user_id,
        winner_subscriber_id=None,
        source_event_id=source_event_id,
        trace_id=trace_id,
        mark_bad_email=mark_bad_email,
    )
    await outbox_repo.enqueue_once(
        operation=OUTBOX_OP_TEYCA_EMAIL_REPAIR_SYNC,
        dedupe_key=dedupe_key_for_email_repair_sync(repair_id=repair_id),
        user_id=loser_user_id,
        payload={
            "repair_id": repair_id,
            "winner_user_id": winner_user_id,
            "winner_subscriber_id": None,
            "mark_bad_email": mark_bad_email,
        },
        trace_id=trace_id,
        source_event_id=source_event_id,
        queue_name=None,
    )
