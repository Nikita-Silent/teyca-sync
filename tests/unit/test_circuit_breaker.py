from __future__ import annotations

import time

import pytest

from app.circuit_breaker import (
    STATE_CLOSED,
    STATE_HALF_OPEN,
    STATE_OPEN,
    CircuitBreaker,
    CircuitBreakerOpenError,
)


@pytest.mark.asyncio
async def test_stays_closed_below_failure_threshold() -> None:
    breaker = CircuitBreaker(name="x", failure_threshold=3, cooldown_seconds=30.0)

    for _ in range(2):
        await breaker.before_call()
        await breaker.record_failure()

    assert breaker.state == STATE_CLOSED


@pytest.mark.asyncio
async def test_opens_after_consecutive_failures_reach_threshold() -> None:
    breaker = CircuitBreaker(name="x", failure_threshold=3, cooldown_seconds=30.0)

    for _ in range(3):
        await breaker.before_call()
        await breaker.record_failure()

    assert breaker.state == STATE_OPEN


@pytest.mark.asyncio
async def test_success_resets_consecutive_failure_count() -> None:
    breaker = CircuitBreaker(name="x", failure_threshold=3, cooldown_seconds=30.0)

    await breaker.before_call()
    await breaker.record_failure()
    await breaker.before_call()
    await breaker.record_failure()
    await breaker.before_call()
    await breaker.record_success()

    # Two more failures after a success should not be enough to trip a
    # threshold of 3 — the counter must have been reset, not just decremented.
    await breaker.before_call()
    await breaker.record_failure()
    await breaker.before_call()
    await breaker.record_failure()

    assert breaker.state == STATE_CLOSED


@pytest.mark.asyncio
async def test_open_circuit_rejects_calls_without_attempting_them() -> None:
    breaker = CircuitBreaker(name="upstream-x", failure_threshold=1, cooldown_seconds=30.0)

    await breaker.before_call()
    await breaker.record_failure()
    assert breaker.state == STATE_OPEN

    with pytest.raises(CircuitBreakerOpenError) as exc_info:
        await breaker.before_call()

    assert "upstream-x" in str(exc_info.value)
    assert exc_info.value.retry_after_seconds > 0


@pytest.mark.asyncio
async def test_transitions_to_half_open_after_cooldown_elapses() -> None:
    breaker = CircuitBreaker(name="x", failure_threshold=1, cooldown_seconds=0.05)

    await breaker.before_call()
    await breaker.record_failure()
    assert breaker.state == STATE_OPEN

    time.sleep(0.06)

    # The cooldown has elapsed: this call must be let through as the probe,
    # not rejected.
    await breaker.before_call()
    assert breaker.state == STATE_HALF_OPEN


@pytest.mark.asyncio
async def test_half_open_probe_success_closes_circuit() -> None:
    breaker = CircuitBreaker(name="x", failure_threshold=1, cooldown_seconds=0.05)

    await breaker.before_call()
    await breaker.record_failure()
    time.sleep(0.06)
    await breaker.before_call()
    assert breaker.state == STATE_HALF_OPEN

    await breaker.record_success()

    assert breaker.state == STATE_CLOSED


@pytest.mark.asyncio
async def test_half_open_probe_failure_reopens_circuit_and_restarts_cooldown() -> None:
    breaker = CircuitBreaker(name="x", failure_threshold=1, cooldown_seconds=0.05)

    await breaker.before_call()
    await breaker.record_failure()
    time.sleep(0.06)
    await breaker.before_call()
    assert breaker.state == STATE_HALF_OPEN

    await breaker.record_failure()

    assert breaker.state == STATE_OPEN
    with pytest.raises(CircuitBreakerOpenError):
        await breaker.before_call()


@pytest.mark.asyncio
async def test_half_open_rejects_concurrent_calls_while_probe_in_flight() -> None:
    breaker = CircuitBreaker(name="x", failure_threshold=1, cooldown_seconds=0.05)

    await breaker.before_call()
    await breaker.record_failure()
    time.sleep(0.06)
    await breaker.before_call()  # first probe let through
    assert breaker.state == STATE_HALF_OPEN

    # A second concurrent caller must not also be let through as a probe.
    with pytest.raises(CircuitBreakerOpenError):
        await breaker.before_call()
