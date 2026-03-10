"""Small shared value-normalization helpers."""

from __future__ import annotations


def to_optional_str(raw: object) -> str | None:
    """Return stripped string or None for non-string/blank input."""
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    return value or None
