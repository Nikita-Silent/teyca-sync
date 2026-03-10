"""Shared helpers for webhook consumers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
import re
from typing import Any

from app.repositories.old_db import OldUserData
from app.schemas.webhook import PassData

SUM_FIELDS: tuple[str, ...] = ("summ", "summ_all", "summ_last", "check_summ")
INT_FIELDS: tuple[str, ...] = ("visits", "visits_all")
EMAIL_RE = re.compile(r"^[A-Za-z0-9!#$%&'*+/=?^_`{|}~.-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+$")
MERGE_TZ = timezone(timedelta(hours=7))


@dataclass(slots=True)
class MergeResult:
    """Result of profile merge operation."""

    profile: dict[str, Any]
    merged: bool


def build_profile_from_pass(pass_data: PassData) -> dict[str, Any]:
    """
    Create a users-table profile dictionary from a PassData webhook payload.
    
    Converts numeric-like fields to optional numeric types where applicable (e.g., bonus, summ*, check_summ to float; visits* to int) and preserves other profile fields for downstream storage.
    
    Parameters:
        pass_data (PassData): Webhook payload representing a user pass.
    
    Returns:
        dict[str, Any]: Profile dictionary with keys:
            email, phone, first_name, last_name, pat_name, birthday, gender,
            barcode, discount, bonus, loyalty_level, summ, summ_all, summ_last,
            check_summ, visits, visits_all, date_last, city.
    """
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
    """
    Merge numeric sum and integer fields from an existing user record into a new profile.
    
    Parameters:
    	profile (dict[str, Any]): Incoming profile data to be augmented.
    	old_data (OldUserData | None): Existing user data to merge from; if None no merge is performed.
    
    Returns:
    	MergeResult: Contains `profile` with summed fields updated when applicable and `merged` set to `true` if any field was augmented, `false` otherwise.
    """
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
    """
    Builds an attributes dictionary for Listmonk from a PassData model.
    
    Serializes `pass_data` to a dict using aliases and excluding None values, copies all resulting key/value pairs except `user_id`, and then sets `user_id` in the returned attributes to `pass_data.user_id`.
    
    Parameters:
        pass_data (PassData): Source model containing subscriber fields.
    
    Returns:
        dict[str, object]: Attributes dictionary suitable for Listmonk, with `user_id` guaranteed present.
    """
    attributes: dict[str, object] = {}
    raw = pass_data.model_dump(by_alias=True, exclude_none=True)
    for key, value in raw.items():
        if key == "user_id":
            continue
        attributes[key] = value
    attributes["user_id"] = pass_data.user_id
    return attributes


def build_merge_key2_value(now: datetime | None = None) -> str:
    """
    Build a human-readable merge marker used as a key2 value.
    
    If `now` is provided it is used as the timestamp; naive datetimes are treated as UTC.
    The timestamp is converted to the module MERGE_TZ and formatted as "merge DD.MM.YYYY HH:MM".
    
    Parameters:
        now (datetime | None): Optional timestamp to use instead of the current time.
    
    Returns:
        str: A string like "merge 31.12.2025 23:59".
    """
    dt = now or datetime.now(UTC)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    dt = dt.astimezone(MERGE_TZ)
    return f"merge {dt.strftime('%d.%m.%Y %H:%M')}"


def is_valid_email(email: str | None) -> bool:
    """
    Validate an email address for basic structural correctness.
    
    Performs observable checks such as trimmed non-empty value, maximum lengths, single '@', non-empty local and domain parts, no leading/trailing or consecutive dots in the local part, and a final regex match.
    
    Returns:
        `true` if the email appears valid, `false` otherwise.
    """
    if email is None:
        return False
    normalized = email.strip()
    if not normalized or len(normalized) > 254 or " " in normalized:
        return False
    if normalized.count("@") != 1:
        return False
    local_part, domain = normalized.split("@", maxsplit=1)
    if not local_part or not domain:
        return False
    if local_part.startswith(".") or local_part.endswith("."):
        return False
    if ".." in local_part or ".." in domain:
        return False
    if len(local_part) > 64:
        return False
    return EMAIL_RE.fullmatch(normalized) is not None


def _to_optional_float(raw: object) -> float | None:
    """
    Convert a raw value to a float when possible, otherwise return None.
    
    Parameters:
        raw (object): Value to convert; may be None, numeric, or a string representation of a number.
    
    Returns:
        float | None: Parsed float if conversion succeeds, `None` if input is `None`, empty, non-numeric, or unsupported.
    """
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
    """
    Convert a raw value to an int when possible, otherwise return None.
    
    Parameters:
        raw (object): A value that may be an int, float, or string representation of an integer.
    
    Returns:
        int | None: The converted integer, or `None` if the input is `None`, an empty/whitespace-only string, or not a parseable integer. Floats are converted by truncation toward zero.
    """
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
