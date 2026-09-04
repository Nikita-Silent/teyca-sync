"""teyca-sync-iil.3: contract tests against real Teyca webhook bodies, plus a
parametrized guard against the next `WebhookPayload`/`PassData` field-type
change quietly turning a routine data quirk (null/number/string) into a 422
that silently drops an event (webhook responses aren't retried, so a 422 here
is a lost event — see teyca-sync-iil.1, teyca-sync-iil.2).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.schemas.webhook import PassData, WebhookPayload

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "teyca_webhooks"
FIXTURE_FILES = sorted(FIXTURES_DIR.glob("*.json"))

# tags is exempt: it's a list, so None/int/str aren't meaningful variants for
# it — its null-hole tolerance ([null], [12, null]) is covered separately in
# tests/unit/test_webhook_schema.py. user_id is exempt: it's the routing key,
# already covered by WebhookEnvelope tests.
SCALAR_FIELDS = [name for name in PassData.model_fields if name not in ("user_id", "tags")]


@pytest.mark.parametrize("fixture_path", FIXTURE_FILES, ids=lambda p: p.stem)
def test_real_teyca_webhook_body_validates(fixture_path: Path) -> None:
    body = json.loads(fixture_path.read_text(encoding="utf-8"))
    event = WebhookPayload.model_validate(body)
    assert event.pass_data.user_id == body["pass"]["user_id"]


def test_fixtures_directory_is_not_empty() -> None:
    """Guards against the parametrize above silently collecting zero cases if
    the fixtures directory is ever emptied or misnamed."""
    assert len(FIXTURE_FILES) >= 3


@pytest.mark.parametrize("field", SCALAR_FIELDS)
@pytest.mark.parametrize("raw_value", [None, 123, "123"], ids=["none", "int", "str"])
def test_scalar_field_accepts_null_number_and_string(field: str, raw_value: object) -> None:
    """Teyca doesn't guarantee a fixed type per field across cards (teyca-sync-iil.2
    found this the hard way for barcode/phone/key1/birthday/bonus). If a future
    schema edit narrows a field's type back down, this fails in CI instead of
    dropping a live UPDATE."""
    WebhookPayload.model_validate({"type": "UPDATE", "pass": {"user_id": 1, field: raw_value}})
