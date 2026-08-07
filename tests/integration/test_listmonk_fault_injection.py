"""Integration test: Listmonk upstream failures against the real circuit
breaker (teyca-sync-9ib), mirroring test_teyca_fault_injection.py.

Per AGENTS.md's testing guidance, Listmonk is faked at the SDK-call layer,
not via HTTP interception (unlike Teyca's respx-mocked httpx transport) —
`ListmonkSDKClient._sdk_call`/`_sdk_call_with_retries` is the layer that
actually holds the retry loop and circuit breaker, so the fake `func` passed
in stands in for the blocking `listmonk` SDK call that would normally run in
a thread. The circuit breaker itself runs for real, same as the Teyca test.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import httpx
import pytest

from app.clients.listmonk import ListmonkClientError, ListmonkSDKClient
from app.config import Settings


def _settings(**overrides: object) -> Settings:
    defaults: dict[str, object] = {
        "listmonk_url": "http://listmonk.example.test",
        "listmonk_user": "user",
        "listmonk_password": "pass",
        "listmonk_request_timeout_seconds": 1.0,
        "listmonk_request_max_retries": 2,
        "listmonk_request_retry_backoff_seconds": 0.0,
        "listmonk_circuit_breaker_failure_threshold": 2,
        "listmonk_circuit_breaker_cooldown_seconds": 30.0,
    }
    defaults.update(overrides)
    return cast(Settings, SimpleNamespace(**defaults))


@pytest.mark.asyncio
async def test_transient_network_error_is_retried_and_succeeds() -> None:
    client = ListmonkSDKClient(_settings())
    attempts = 0

    def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise httpx.ConnectError("connection reset by peer")
        return "ok"

    result = await client._sdk_call(flaky, action="flaky_call")

    assert result == "ok"
    assert attempts == 2


@pytest.mark.asyncio
async def test_non_retryable_error_fails_on_first_attempt() -> None:
    client = ListmonkSDKClient(_settings())
    attempts = 0

    def always_fails() -> None:
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectError("connection reset by peer")

    with pytest.raises(httpx.ConnectError):
        await client._sdk_call(always_fails, action="create_subscriber", retryable=False)

    assert attempts == 1


@pytest.mark.asyncio
async def test_retries_exhausted_then_raises() -> None:
    client = ListmonkSDKClient(_settings(listmonk_request_max_retries=2))
    attempts = 0

    def always_times_out() -> None:
        nonlocal attempts
        attempts += 1
        raise httpx.ReadTimeout("timed out")

    with pytest.raises(httpx.ReadTimeout):
        await client._sdk_call(always_times_out, action="get_subscriber")

    # Initial attempt + 2 retries.
    assert attempts == 3


@pytest.mark.asyncio
async def test_circuit_breaker_opens_after_repeated_failures_and_stops_real_calls() -> None:
    client = ListmonkSDKClient(
        _settings(listmonk_request_max_retries=0, listmonk_circuit_breaker_failure_threshold=2)
    )
    attempts = 0

    def always_fails() -> None:
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectError("connection reset by peer")

    with pytest.raises(httpx.ConnectError):
        await client._sdk_call(always_fails, action="upsert_subscriber", retryable=False)
    with pytest.raises(httpx.ConnectError):
        await client._sdk_call(always_fails, action="upsert_subscriber", retryable=False)
    assert attempts == 2

    with pytest.raises(ListmonkClientError, match="Circuit breaker"):
        await client._sdk_call(always_fails, action="upsert_subscriber", retryable=False)

    # The breaker rejected the third call before it reached the (fake) SDK function.
    assert attempts == 2
