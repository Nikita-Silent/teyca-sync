"""Incoming webhook payload from Teyca. type + pass (any extra fields from Teyca allowed)."""

from typing import Literal

from pydantic import BaseModel, Field, field_validator


class PassData(BaseModel):
    """Loyalty card data (body.pass). Extra fields from Teyca are preserved and forwarded."""

    # coerce_numbers_to_str (teyca-sync-iil.2): Teyca's str fields (barcode, phone,
    # key1, birthday, ...) aren't guaranteed to arrive as strings — pydantic v2
    # doesn't coerce int/float -> str by default, so a single numeric field would
    # 422 the whole webhook the same way tags did (teyca-sync-iil.1). bool is
    # still rejected (not coerced to "True"/"False"), which is what we want.
    model_config = {"extra": "allow", "coerce_numbers_to_str": True}

    user_id: int
    email: str | None = None
    phone: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    pat_name: str | None = None
    fio: str | None = None
    birthday: str | None = None
    gender: str | None = None
    barcode: str | None = None
    discount: str | None = None
    bonus: float | None = None
    loyalty_level: str | None = None
    summ: float | str | None = None
    summ_all: float | str | None = None
    summ_last: float | str | None = None
    check_summ: float | None = None
    visits: int | str | None = None
    visits_all: int | str | None = None
    date_last: str | None = None
    city: str | None = None
    referal: str | None = None
    tags: list[int] | None = None
    template: str | None = None
    key1: str | None = None
    key2: str | None = None
    # остальные поля Teyca попадают в model_dump() за счёт extra="allow"

    @field_validator("tags", mode="before")
    @classmethod
    def _drop_invalid_tags(cls, value: object) -> object:
        """Teyca sends null holes in tags (e.g. [null], [12, null]) — drop them
        instead of failing the whole webhook (teyca-sync-iil.1: a strict
        list[int] here turned a harmless data quirk into 422s that silently
        dropped UPDATE events, since webhook responses aren't retried)."""
        if not isinstance(value, list):
            return value
        return [item for item in value if isinstance(item, int) and not isinstance(item, bool)]

    @field_validator("bonus", "check_summ", mode="before")
    @classmethod
    def _blank_string_to_none(cls, value: object) -> object:
        """bonus/check_summ have no str alternative in the type (unlike summ*),
        so an empty string from Teyca fails float_parsing (teyca-sync-iil.2)."""
        if isinstance(value, str) and value.strip() == "":
            return None
        return value


class WebhookPayload(BaseModel):
    """Top-level webhook body: type + pass."""

    type: Literal["CREATE", "UPDATE", "DELETE"]
    pass_data: PassData = Field(..., alias="pass")

    model_config = {"populate_by_name": True}


class _EnvelopePassData(BaseModel):
    """Only what's needed to route the event: the primary key."""

    model_config = {"extra": "ignore"}

    user_id: int


class WebhookEnvelope(BaseModel):
    """Ingress-only gate (teyca-sync-iil.4): checks just `type` and `pass.user_id` —
    the two fields the inbox needs to route and dedupe the event. Everything else
    in `pass` is accepted as-is and validated later by `WebhookPayload` in the
    consumer, where a failure is a retryable/inspectable `dead` row instead of a
    422 that silently drops the event (webhook responses aren't retried)."""

    type: Literal["CREATE", "UPDATE", "DELETE"]
    pass_data: _EnvelopePassData = Field(..., alias="pass")

    model_config = {"populate_by_name": True}
