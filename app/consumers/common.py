"""Shared helpers for webhook consumers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from app.repositories.old_db import OldUserData
from app.schemas.webhook import PassData

SUM_FIELDS: tuple[str, ...] = ("summ", "summ_all", "summ_last", "check_summ")
INT_FIELDS: tuple[str, ...] = ("visits", "visits_all")


@dataclass(slots=True)
class MergeResult:
    """Result of profile merge operation."""

    profile: dict[str, Any]
    merged: bool


def build_profile_from_pass(pass_data: PassData) -> dict[str, Any]:
    """Map webhook pass payload into users table profile dict."""
    return {
        "email": pass_data.email,
        "phone": pass_data.phone,
        "first_name": pass_data.first_name,
        "last_name": pass_data.last_name,
        "pat_name": pass_data.pat_name,
        "birthday": pass_data.birthday,
        "gender": pass_data.gender,
        "barcode": pass_data.barcode,
        "discount": pass_data.discount,
        "bonus": _to_optional_float(pass_data.bonus),
        "loyalty_level": pass_data.loyalty_level,
        "summ": _to_optional_float(pass_data.summ),
        "summ_all": _to_optional_float(pass_data.summ_all),
        "summ_last": _to_optional_float(pass_data.summ_last),
        "check_summ": _to_optional_float(pass_data.check_summ),
        "visits": _to_optional_int(pass_data.visits),
        "visits_all": _to_optional_int(pass_data.visits_all),
        "date_last": pass_data.date_last,
        "city": pass_data.city,
    }


def merge_profile_with_old_data(profile: dict[str, Any], old_data: OldUserData | None) -> MergeResult:
    """Apply merge-by-sum rules for numeric fields."""
    if old_data is None:
        return MergeResult(profile=profile, merged=False)

    merged_profile = dict(profile)
    merged = False

    for field in SUM_FIELDS:
        base_value = _to_optional_float(merged_profile.get(field))
        old_value = _to_optional_float(getattr(old_data, field))
        if old_value is None:
            continue
        merged_profile[field] = (base_value or 0.0) + old_value
        merged = True

    for field in INT_FIELDS:
        base_value = _to_optional_int(merged_profile.get(field))
        old_value = _to_optional_int(getattr(old_data, field))
        if old_value is None:
            continue
        merged_profile[field] = (base_value or 0) + old_value
        merged = True

    return MergeResult(profile=merged_profile, merged=merged)


def build_listmonk_attributes(pass_data: PassData) -> dict[str, object]:
    """Construct attributes payload for Listmonk."""
    attributes: dict[str, object] = {}
    raw = pass_data.model_dump(by_alias=True, exclude_none=True)
    for key, value in raw.items():
        if key == "user_id":
            continue
        attributes[key] = value
    attributes["user_id"] = pass_data.user_id
    return attributes


def build_merge_key2_value(now: datetime | None = None) -> str:
    """Build human-readable key2 marker for successful merge."""
    dt = now or datetime.now().astimezone()
    return f"merge {dt.strftime('%d.%m.%Y %H:%M')}"


def _to_optional_float(raw: object) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, (float, int)):
        return float(raw)
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _to_optional_int(raw: object) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
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
