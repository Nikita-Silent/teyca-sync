from __future__ import annotations

from datetime import datetime

import pytest

from app.policies.email_duplicate_policy import (
    EmailDuplicateCandidate,
    resolve_email_duplicate_group,
)


def _candidate(
    *,
    user_id: int,
    phone: str | None = None,
    date_last: str | None = None,
    updated_at: datetime | None = None,
) -> EmailDuplicateCandidate:
    return EmailDuplicateCandidate(
        user_id=user_id,
        phone=phone,
        date_last=date_last,
        updated_at=updated_at,
    )


def test_same_phone_winner_keeps_email_loser_cleared_without_bad_email_mark() -> None:
    candidates = [
        _candidate(user_id=1, phone="+79990000000", date_last="2026-07-01"),
        _candidate(user_id=2, phone="+79990000000", date_last="2026-07-20"),
    ]

    resolution = resolve_email_duplicate_group(candidates)

    assert resolution.winner_user_id == 2
    assert resolution.loser_user_ids == [1]
    assert resolution.mark_bad_email is False


def test_different_phones_winner_keeps_email_loser_marked_bad_email() -> None:
    candidates = [
        _candidate(user_id=1, phone="+79990000001", date_last="2026-07-01"),
        _candidate(user_id=2, phone="+79990000002", date_last="2026-07-20"),
    ]

    resolution = resolve_email_duplicate_group(candidates)

    assert resolution.winner_user_id == 2
    assert resolution.loser_user_ids == [1]
    assert resolution.mark_bad_email is True


def test_group_of_many_candidates_has_exactly_one_winner() -> None:
    candidates = [
        _candidate(user_id=1, phone="+79990000001", date_last="2026-06-01"),
        _candidate(user_id=2, phone="+79990000002", date_last="2026-07-10"),
        _candidate(user_id=3, phone="+79990000003", date_last="2026-07-30"),
        _candidate(user_id=4, phone="+79990000004", date_last=None),
        _candidate(user_id=5, phone="+79990000005", date_last="2026-01-01"),
    ]

    resolution = resolve_email_duplicate_group(candidates)

    assert resolution.winner_user_id == 3
    assert sorted(resolution.loser_user_ids) == [1, 2, 4, 5]


def test_tiebreak_falls_back_to_updated_at_when_date_last_missing() -> None:
    candidates = [
        _candidate(
            user_id=1, phone="+79990000001", date_last=None, updated_at=datetime(2026, 1, 1)
        ),
        _candidate(
            user_id=2, phone="+79990000002", date_last=None, updated_at=datetime(2026, 7, 1)
        ),
    ]

    resolution = resolve_email_duplicate_group(candidates)

    assert resolution.winner_user_id == 2


def test_tiebreak_falls_back_to_smaller_user_id_when_no_activity_at_all() -> None:
    candidates = [
        _candidate(user_id=42, phone="+79990000001"),
        _candidate(user_id=7, phone="+79990000002"),
    ]

    resolution = resolve_email_duplicate_group(candidates)

    assert resolution.winner_user_id == 7
    assert resolution.loser_user_ids == [42]


def test_date_last_beats_updated_at_when_both_present() -> None:
    candidates = [
        _candidate(
            user_id=1,
            phone="+79990000001",
            date_last="2026-01-01",
            updated_at=datetime(2026, 7, 30),
        ),
        _candidate(
            user_id=2,
            phone="+79990000002",
            date_last="2026-07-30",
            updated_at=datetime(2026, 1, 1),
        ),
    ]

    resolution = resolve_email_duplicate_group(candidates)

    assert resolution.winner_user_id == 2


def test_phone_none_counts_as_different_person() -> None:
    candidates = [
        _candidate(user_id=1, phone=None, date_last="2026-07-01"),
        _candidate(user_id=2, phone=None, date_last="2026-07-20"),
    ]

    resolution = resolve_email_duplicate_group(candidates)

    assert resolution.mark_bad_email is True


def test_requires_at_least_two_candidates() -> None:
    with pytest.raises(ValueError):
        resolve_email_duplicate_group([_candidate(user_id=1)])
