"""Deterministic resolution policy for duplicate email groups (Р5, Р6).

Rules (docs/reverse-engineering-plan.md, section 5, stage 1):
- same phone -> one person: winner keeps the email, loser is cleared
  without a `bad email` mark.
- different (or missing) phone -> different people: winner keeps the
  email, loser is cleared and marked `bad email`.
- winner tiebreak: most recent `date_last`, then `updated_at`, then the
  smaller `user_id`. Missing values sort last.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class EmailDuplicateCandidate:
    """One row participating in a duplicate-email group."""

    user_id: int
    phone: str | None = None
    date_last: str | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class EmailDuplicateResolution:
    """Resolved winner/losers for one duplicate-email group."""

    winner_user_id: int
    loser_user_ids: list[int]
    mark_bad_email: bool


def _parse_date_last(raw: str | None) -> datetime | None:
    """Best-effort parse of the free-form `date_last` string field."""
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _activity_rank(candidate: EmailDuplicateCandidate) -> tuple[int, datetime]:
    """Rank a candidate's activity: higher sorts as more recently active.

    `date_last` outranks `updated_at`, which outranks no activity at all.
    """
    date_last = _parse_date_last(candidate.date_last)
    if date_last is not None:
        return (2, date_last)
    if candidate.updated_at is not None:
        return (1, candidate.updated_at)
    return (0, datetime.min)


def _same_person(
    winner: EmailDuplicateCandidate,
    others: list[EmailDuplicateCandidate],
) -> bool:
    """True only when every candidate shares the same non-empty phone."""
    if not winner.phone:
        return False
    return all(other.phone == winner.phone for other in others)


def resolve_email_duplicate_group(
    candidates: list[EmailDuplicateCandidate],
) -> EmailDuplicateResolution:
    """Resolve one duplicate-email group into a winner and losers.

    Deterministic — no manual review, no external calls.
    """
    if len(candidates) < 2:
        raise ValueError("A duplicate-email group requires at least two candidates")

    winner = max(
        candidates,
        key=lambda candidate: (_activity_rank(candidate), -candidate.user_id),
    )
    losers = [candidate for candidate in candidates if candidate.user_id != winner.user_id]

    return EmailDuplicateResolution(
        winner_user_id=winner.user_id,
        loser_user_ids=[loser.user_id for loser in losers],
        mark_bad_email=not _same_person(winner, losers),
    )
