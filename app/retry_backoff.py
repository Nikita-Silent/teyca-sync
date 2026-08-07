"""Shared exponential backoff formula for retry scheduling."""

from __future__ import annotations


def compute_retry_delay_ms(*, retry_count: int, base_delay_ms: int, max_delay_ms: int) -> int:
    bounded_retry_count = max(1, retry_count)
    delay_ms = max(1, base_delay_ms) * (2 ** (bounded_retry_count - 1))
    return min(delay_ms, max(1, max_delay_ms))
