"""Incoming webhook payload from Teyca. type + pass (any extra fields from Teyca allowed)."""

from typing import Literal

from pydantic import BaseModel, Field


class PassData(BaseModel):
    """Loyalty card data (body.pass). Extra fields from Teyca are preserved and forwarded."""

    model_config = {"extra": "allow"}

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
    template: str | None = None
    key1: str | None = None
    key2: str | None = None
    # остальные поля Teyca попадают в model_dump() за счёт extra="allow"


class WebhookPayload(BaseModel):
    """Top-level webhook body: type + pass."""

    type: Literal["CREATE", "UPDATE", "DELETE"]
    pass_data: PassData = Field(..., alias="pass")

    model_config = {"populate_by_name": True}
