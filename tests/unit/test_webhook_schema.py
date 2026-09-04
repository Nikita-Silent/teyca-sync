"""Unit tests for WebhookPayload/PassData tolerance to Teyca's real-world payload quirks
(teyca-sync-iil.1: null holes in `tags` were rejected by the schema and turned into 422s
that silently dropped UPDATE events, since webhook responses aren't retried)."""

import pytest
from pydantic import ValidationError

from app.schemas.webhook import WebhookPayload


def _payload(**pass_overrides: object) -> dict:
    return {"type": "UPDATE", "pass": {"user_id": 1, **pass_overrides}}


def test_tags_with_null_hole_is_dropped() -> None:
    event = WebhookPayload.model_validate(_payload(tags=[None]))
    assert event.pass_data.tags == []


def test_tags_with_mixed_null_and_int_keeps_valid_entries() -> None:
    event = WebhookPayload.model_validate(_payload(tags=[12, None]))
    assert event.pass_data.tags == [12]


def test_tags_all_valid_ints_unchanged() -> None:
    event = WebhookPayload.model_validate(_payload(tags=[1, 2, 3]))
    assert event.pass_data.tags == [1, 2, 3]


def test_tags_none_stays_none() -> None:
    event = WebhookPayload.model_validate(_payload(tags=None))
    assert event.pass_data.tags is None


def test_tags_missing_stays_none() -> None:
    event = WebhookPayload.model_validate(_payload())
    assert event.pass_data.tags is None


def test_tags_not_a_list_passes_through_and_fails_normally() -> None:
    """Non-list tags aren't Teyca's null-hole quirk — still a real schema error."""
    with pytest.raises(ValidationError):
        WebhookPayload.model_validate(_payload(tags={"a": 1}))


def test_tags_bool_entries_are_dropped_not_coerced() -> None:
    """bool is a subclass of int in Python — must not slip through as 0/1."""
    event = WebhookPayload.model_validate(_payload(tags=[True, 5, False]))
    assert event.pass_data.tags == [5]


@pytest.mark.parametrize(
    ("field", "raw", "expected"),
    [
        ("barcode", 1234567, "1234567"),
        ("phone", 79001234567, "79001234567"),
        ("key1", 0, "0"),
        ("birthday", 20000101, "20000101"),
        ("discount", 5.5, "5.5"),
    ],
)
def test_numeric_scalar_str_fields_are_coerced_to_string(
    field: str, raw: object, expected: str
) -> None:
    """teyca-sync-iil.2: pydantic v2 doesn't coerce int/float -> str by default,
    so a numeric barcode/phone/key1/birthday/discount would 422 the whole
    webhook the same way `tags` did (teyca-sync-iil.1)."""
    event = WebhookPayload.model_validate(_payload(**{field: raw}))
    assert getattr(event.pass_data, field) == expected


def test_scalar_str_field_bool_is_still_rejected() -> None:
    """coerce_numbers_to_str must not turn a bool into "True"/"False"."""
    with pytest.raises(ValidationError):
        WebhookPayload.model_validate(_payload(barcode=True))


@pytest.mark.parametrize("field", ["bonus", "check_summ"])
def test_blank_string_in_float_only_field_becomes_none(field: str) -> None:
    """bonus/check_summ have no str alternative (unlike summ/summ_all/summ_last),
    so an empty string from Teyca used to fail float_parsing (teyca-sync-iil.2)."""
    event = WebhookPayload.model_validate(_payload(**{field: ""}))
    assert getattr(event.pass_data, field) is None


@pytest.mark.parametrize("field", ["bonus", "check_summ"])
def test_numeric_string_in_float_only_field_still_parses(field: str) -> None:
    event = WebhookPayload.model_validate(_payload(**{field: "300"}))
    assert getattr(event.pass_data, field) == 300.0
