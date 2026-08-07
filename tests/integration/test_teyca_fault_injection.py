"""Integration test: Teyca upstream failures (503/429/timeout/connection
drop) against the real Postgres-backed call budget and circuit breaker
(teyca-sync-8kh, teyca-sync-cex).

respx intercepts the real httpx transport TeycaClient talks over, so the
retry loop, the 429 status_code plumbing, and the circuit breaker all run
for real — only the network hop to Teyca itself is faked. The call budget
lives in Postgres (teyca_call_budget), so this also proves the limiter
survives a real commit/rollback cycle, which an AsyncMock session can't.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import httpx
import pytest
import respx
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from app.clients.teyca import BonusOperation, TeycaAPIError, TeycaClient, build_teyca_client
from app.config import Settings

TEYCA_BASE_URL = "https://teyca.example.test"


def _settings(**overrides: object) -> Settings:
    defaults: dict[str, object] = {
        "teyca_base_url": TEYCA_BASE_URL,
        "teyca_api_key": "api-key",
        "teyca_token": "token-1",
        "teyca_rate_limit_per_second": 1000,
        "teyca_rate_limit_per_minute": 1000,
        "teyca_rate_limit_per_hour": 1000,
        "teyca_rate_limit_per_day": 1000,
        "teyca_request_max_retries": 2,
        "teyca_request_retry_backoff_seconds": 0.0,
        "teyca_connect_timeout_seconds": 1.0,
        "teyca_read_timeout_seconds": 1.0,
        "teyca_write_timeout_seconds": 1.0,
        "teyca_pool_timeout_seconds": 1.0,
        "teyca_circuit_breaker_failure_threshold": 2,
        "teyca_circuit_breaker_cooldown_seconds": 30.0,
    }
    defaults.update(overrides)
    return cast(Settings, SimpleNamespace(**defaults))


def _client(engine: AsyncEngine, **settings_overrides: object) -> TeycaClient:
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    return build_teyca_client(_settings(**settings_overrides), session_factory=session_factory)


@pytest.mark.asyncio
@respx.mock
async def test_503_then_200_is_retried_and_succeeds(engine: AsyncEngine) -> None:
    route = respx.post(f"{TEYCA_BASE_URL}/v1/token-1/passes/1/bonuses").mock(
        side_effect=[httpx.Response(503), httpx.Response(200, text="ok")]
    )
    client = _client(engine)

    await client.accrue_bonuses(user_id=1, bonuses=[BonusOperation.one_shot(value="1")])

    assert route.call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_429_raises_rate_limited_teyca_api_error(engine: AsyncEngine) -> None:
    route = respx.post(f"{TEYCA_BASE_URL}/v1/token-1/passes/2/bonuses").mock(
        return_value=httpx.Response(429, text="slow down")
    )
    client = _client(engine)

    with pytest.raises(TeycaAPIError) as exc_info:
        await client.accrue_bonuses(user_id=2, bonuses=[BonusOperation.one_shot(value="1")])

    assert exc_info.value.is_rate_limited is True
    # 429 isn't in the retryable-5xx path — one real call, no wasted retries.
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_connection_drop_is_retried_then_raises_after_exhausting_retries(
    engine: AsyncEngine,
) -> None:
    route = respx.post(f"{TEYCA_BASE_URL}/v1/token-1/passes/3/bonuses").mock(
        side_effect=httpx.ConnectError("connection reset by peer")
    )
    client = _client(engine, teyca_request_max_retries=2)

    with pytest.raises(httpx.ConnectError):
        await client.accrue_bonuses(user_id=3, bonuses=[BonusOperation.one_shot(value="1")])

    # Initial attempt + 2 retries.
    assert route.call_count == 3


@pytest.mark.asyncio
@respx.mock
async def test_read_timeout_is_retried_then_raises_after_exhausting_retries(
    engine: AsyncEngine,
) -> None:
    route = respx.post(f"{TEYCA_BASE_URL}/v1/token-1/passes/4/bonuses").mock(
        side_effect=httpx.ReadTimeout("timed out")
    )
    client = _client(engine, teyca_request_max_retries=1)

    with pytest.raises(httpx.ReadTimeout):
        await client.accrue_bonuses(user_id=4, bonuses=[BonusOperation.one_shot(value="1")])

    assert route.call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_circuit_breaker_opens_after_repeated_5xx_and_stops_real_calls(
    engine: AsyncEngine,
) -> None:
    route = respx.post(f"{TEYCA_BASE_URL}/v1/token-1/passes/5/bonuses").mock(
        return_value=httpx.Response(500, text="upstream down")
    )
    # No internal per-call retries, so each accrue_bonuses call is exactly
    # one HTTP attempt from the breaker's point of view.
    client = _client(
        engine,
        teyca_request_max_retries=0,
        teyca_circuit_breaker_failure_threshold=2,
    )
    op = BonusOperation.one_shot(value="1")

    with pytest.raises(TeycaAPIError):
        await client.accrue_bonuses(user_id=5, bonuses=[op])
    with pytest.raises(TeycaAPIError):
        await client.accrue_bonuses(user_id=5, bonuses=[op])
    assert route.call_count == 2

    with pytest.raises(TeycaAPIError, match="Circuit breaker"):
        await client.accrue_bonuses(user_id=5, bonuses=[op])

    # The breaker rejected the third call before it reached the network.
    assert route.call_count == 2
