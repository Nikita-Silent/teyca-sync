from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import HTTPException

from app.api.auth import verify_webhook_token
from app.clients.teyca import (
    BonusOperation,
    PostgresCallBudgetLimiter,
    TeycaAPIError,
    TeycaClient,
    TeycaRateLimitBusyError,
    _build_budget_limits,
    _extract_operation_items,
    build_teyca_client,
    build_teyca_rate_limiter,
)
from app.config import Settings
from app.consumers.common import (
    _to_optional_float,
    _to_optional_int,
    _to_optional_int_list,
    _to_optional_str,
    build_listmonk_attributes,
    build_merge_key2_value,
    build_profile_from_pass,
    is_valid_email,
    merge_profile_with_old_data,
)
from app.repositories.old_db import OldUserData
from app.schemas.webhook import PassData
from app.workers import run_consent_sync, run_listmonk_reconcile


@pytest.mark.asyncio
async def test_verify_webhook_token_all_branches() -> None:
    with patch(
        "app.api.auth.get_settings",
        return_value=SimpleNamespace(webhook_auth_enabled=False, webhook_auth_token=""),
    ):
        assert await verify_webhook_token(None) is None

    with patch(
        "app.api.auth.get_settings",
        return_value=SimpleNamespace(webhook_auth_enabled=True, webhook_auth_token=""),
    ):
        with pytest.raises(HTTPException) as exc:
            await verify_webhook_token("x")
        assert exc.value.status_code == 503

    with patch(
        "app.api.auth.get_settings",
        return_value=SimpleNamespace(webhook_auth_enabled=True, webhook_auth_token="secret"),
    ):
        with pytest.raises(HTTPException) as exc:
            await verify_webhook_token(None)
        assert exc.value.status_code == 401

    with patch(
        "app.api.auth.get_settings",
        return_value=SimpleNamespace(webhook_auth_enabled=True, webhook_auth_token="secret"),
    ):
        with pytest.raises(HTTPException) as exc:
            await verify_webhook_token("wrong")
        assert exc.value.status_code == 403

    with patch(
        "app.api.auth.get_settings",
        return_value=SimpleNamespace(webhook_auth_enabled=True, webhook_auth_token="secret"),
    ):
        assert await verify_webhook_token("Bearer secret") is None


def test_common_helpers_cover_numeric_and_merge_paths() -> None:
    pass_data = PassData.model_validate(
        {
            "user_id": 42,
            "email": "user@example.com",
            "phone": "79039859055",
            "summ": "10.5",
            "visits": "2",
            "bonus": "100",
            "referal": "  4243447  ",
            "tags": [892, 899],
        }
    )
    profile = build_profile_from_pass(pass_data)
    assert profile["summ"] == 10.5
    assert profile["visits"] == 2
    assert profile["referal"] == "4243447"
    assert profile["tags"] == [892, 899]

    merged = merge_profile_with_old_data(
        profile,
        OldUserData(summ=1.5, visits=3, check_summ=None),
    )
    assert merged.merged is True
    assert merged.profile["summ"] == 12.0
    assert merged.profile["visits"] == 5

    not_merged = merge_profile_with_old_data(profile, None)
    assert not_merged.merged is False

    attrs = build_listmonk_attributes(pass_data)
    assert attrs["user_id"] == 42
    assert "user_id" in attrs

    assert _to_optional_float(None) is None
    assert _to_optional_float(" ") is None
    assert _to_optional_float("bad") is None
    assert _to_optional_float("1.2") == 1.2
    assert _to_optional_float(object()) is None
    assert _to_optional_int(None) is None
    assert _to_optional_int(" ") is None
    assert _to_optional_int("bad") is None
    assert _to_optional_int(1.9) == 1
    assert _to_optional_int(object()) is None
    assert _to_optional_str("  abc  ") == "abc"
    assert _to_optional_str(" ") is None
    assert _to_optional_int_list([1, "2"]) == [1, 2]
    assert _to_optional_int_list(["bad"]) is None
    assert is_valid_email("user@example.com") is True
    assert is_valid_email("bad..mail@example.com") is False
    assert is_valid_email("bad.mail@") is False

    assert (
        build_merge_key2_value(datetime(2026, 3, 6, 12, 1, tzinfo=UTC)) == "merge 06.03.2026 19:01"
    )


@pytest.mark.asyncio
async def test_teyca_client_all_branches_with_injected_http_client() -> None:
    settings = SimpleNamespace(
        teyca_base_url="https://api.example.com/",
        teyca_api_key="api-key",
        teyca_token="token-1",
    )
    http_client = AsyncMock()
    http_client.post.return_value = SimpleNamespace(status_code=200, text="ok")
    http_client.put.return_value = SimpleNamespace(status_code=200, text="ok")
    rate_limiter = AsyncMock()
    client = TeycaClient(
        settings=cast(Settings, settings),
        http_client=http_client,
        rate_limiter=rate_limiter,
    )

    op = BonusOperation.one_shot(value="10")
    assert op.to_dict()["value"] == "10"

    await client.accrue_bonuses(user_id=10, bonuses=[op])
    await client.update_pass_fields(user_id=10, fields={"key1": "confirmed"})

    http_client.post.return_value = SimpleNamespace(status_code=400, text="bad")
    with pytest.raises(TeycaAPIError):
        await client.accrue_bonuses(user_id=10, bonuses=[op])

    http_client.put.return_value = SimpleNamespace(status_code=500, text="bad")
    with pytest.raises(TeycaAPIError):
        await client.update_pass_fields(user_id=10, fields={"k": "v"})

    assert rate_limiter.acquire.await_count == 4


@pytest.mark.asyncio
async def test_teyca_client_list_operations_returns_items_and_respects_budget() -> None:
    settings = SimpleNamespace(
        teyca_base_url="https://api.example.com/",
        teyca_api_key="api-key",
        teyca_token="token-1",
    )
    http_client = AsyncMock()
    http_client.post.return_value = SimpleNamespace(
        status_code=200,
        text="ok",
        json=lambda: [{"user_id": 10, "value": "100.0"}],
    )
    rate_limiter = AsyncMock()
    client = TeycaClient(
        settings=cast(Settings, settings),
        http_client=http_client,
        rate_limiter=rate_limiter,
    )

    items = await client.list_operations(user_ids=[10, 11])

    assert items == [{"user_id": 10, "value": "100.0"}]
    rate_limiter.acquire.assert_awaited_once()
    call_kwargs = http_client.post.await_args.kwargs
    assert call_kwargs["json"] == {"filters": {"user_ids": [10, 11]}, "order": "desc"}


@pytest.mark.asyncio
async def test_teyca_client_list_operations_empty_user_ids_skips_request() -> None:
    client = TeycaClient(
        settings=cast(
            Settings,
            SimpleNamespace(
                teyca_base_url="https://api.example.com/",
                teyca_api_key="api-key",
                teyca_token="token-1",
            ),
        ),
        http_client=AsyncMock(),
        rate_limiter=AsyncMock(),
    )
    assert await client.list_operations(user_ids=[]) == []


@pytest.mark.asyncio
async def test_teyca_client_list_operations_raises_on_error_status() -> None:
    http_client = AsyncMock()
    http_client.post.return_value = SimpleNamespace(status_code=500, text="boom")
    client = TeycaClient(
        settings=cast(
            Settings,
            SimpleNamespace(
                teyca_base_url="https://api.example.com/",
                teyca_api_key="api-key",
                teyca_token="token-1",
            ),
        ),
        http_client=http_client,
        rate_limiter=AsyncMock(),
    )
    with pytest.raises(TeycaAPIError):
        await client.list_operations(user_ids=[1])


@pytest.mark.asyncio
async def test_teyca_client_circuit_breaker_opens_after_repeated_5xx() -> None:
    """teyca-sync-cex: repeated upstream 5xx must trip the breaker so further
    calls fail fast without another real HTTP round-trip to a struggling Teyca."""
    settings = SimpleNamespace(
        teyca_base_url="https://api.example.com/",
        teyca_api_key="api-key",
        teyca_token="token-1",
        teyca_request_max_retries=0,
        teyca_circuit_breaker_failure_threshold=2,
        teyca_circuit_breaker_cooldown_seconds=30.0,
    )
    http_client = AsyncMock()
    http_client.post.return_value = SimpleNamespace(status_code=500, text="boom")
    client = TeycaClient(
        settings=cast(Settings, settings),
        http_client=http_client,
        rate_limiter=AsyncMock(),
    )
    op = BonusOperation.one_shot(value="1")

    with pytest.raises(TeycaAPIError):
        await client.accrue_bonuses(user_id=1, bonuses=[op])
    with pytest.raises(TeycaAPIError):
        await client.accrue_bonuses(user_id=1, bonuses=[op])
    assert http_client.post.await_count == 2

    with pytest.raises(TeycaAPIError, match="Circuit breaker"):
        await client.accrue_bonuses(user_id=1, bonuses=[op])

    # The breaker rejected the third call before attempting it — no new HTTP call.
    assert http_client.post.await_count == 2


def test_extract_operation_items_handles_wrapped_and_plain_shapes() -> None:
    assert _extract_operation_items([{"user_id": 1}, "bad"]) == [{"user_id": 1}]
    assert _extract_operation_items({"items": [{"user_id": 2}]}) == [{"user_id": 2}]
    assert _extract_operation_items({"data": [{"user_id": 3}]}) == [{"user_id": 3}]
    assert _extract_operation_items({"unexpected": "shape"}) == []
    assert _extract_operation_items("not a container") == []


def test_teyca_client_settings_validation() -> None:
    settings = SimpleNamespace(
        teyca_base_url="https://api.example.com",
        teyca_api_key="",
        teyca_token="",
    )
    client = TeycaClient(
        settings=cast(Settings, settings),
        rate_limiter=AsyncMock(),
    )
    with pytest.raises(TeycaAPIError):
        client._get_headers()


@pytest.mark.asyncio
async def test_teyca_client_uses_internal_httpx_client_when_not_injected() -> None:
    settings = SimpleNamespace(
        teyca_base_url="https://api.example.com/",
        teyca_api_key="api-key",
        teyca_token="token-1",
    )
    client = TeycaClient(
        settings=cast(Settings, settings),
        http_client=None,
        rate_limiter=AsyncMock(),
    )

    httpx_client = AsyncMock()
    httpx_client.post.return_value = SimpleNamespace(status_code=200, text="ok")
    httpx_client.put.return_value = SimpleNamespace(status_code=200, text="ok")

    with patch(
        "app.clients.teyca.httpx.AsyncClient", return_value=httpx_client
    ) as async_client_cls:
        await client.accrue_bonuses(user_id=1, bonuses=[BonusOperation.one_shot(value="1")])
        await client.update_pass_fields(user_id=1, fields={"key1": "confirmed"})

    assert httpx_client.post.await_count == 1
    assert httpx_client.put.await_count == 1
    # The client is created once and reused across calls, not per-request.
    assert async_client_cls.call_count == 1
    timeout = async_client_cls.call_args.kwargs["timeout"]
    assert isinstance(timeout, httpx.Timeout)


@pytest.mark.asyncio
async def test_build_budget_limits_uses_real_teyca_defaults() -> None:
    settings = SimpleNamespace()

    limits = _build_budget_limits(cast(Settings, settings))

    assert limits == (
        ("second", 1, 5),
        ("minute", 60, 50),
        ("hour", 3600, 500),
        ("day", 86400, 5000),
    )


def test_build_budget_limits_reads_configured_values() -> None:
    settings = SimpleNamespace(
        teyca_rate_limit_per_second=1,
        teyca_rate_limit_per_minute=2,
        teyca_rate_limit_per_hour=3,
        teyca_rate_limit_per_day=4,
    )

    limits = _build_budget_limits(cast(Settings, settings))

    assert limits == (("second", 1, 1), ("minute", 60, 2), ("hour", 3600, 3), ("day", 86400, 4))


@pytest.mark.asyncio
async def test_postgres_call_budget_limiter_reserves_when_allowed() -> None:
    session = AsyncMock()
    session.__aenter__.return_value = session
    session.__aexit__.return_value = False
    session_factory = MagicMock(return_value=session)

    with patch(
        "app.clients.teyca.TeycaCallBudgetRepository.try_reserve",
        new=AsyncMock(return_value=SimpleNamespace(allowed=True, retry_after_seconds=0.0)),
    ):
        limiter = PostgresCallBudgetLimiter(
            session_factory=session_factory,
            limits=(("minute", 60, 50),),
        )
        await limiter.acquire()

    session.commit.assert_awaited_once()
    session.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_postgres_call_budget_limiter_raises_without_sleeping_when_exhausted() -> None:
    """teyca-sync-3al: an exhausted budget must fail fast, never block/sleep."""
    session = AsyncMock()
    session.__aenter__.return_value = session
    session.__aexit__.return_value = False
    session_factory = MagicMock(return_value=session)

    with (
        patch(
            "app.clients.teyca.TeycaCallBudgetRepository.try_reserve",
            new=AsyncMock(return_value=SimpleNamespace(allowed=False, retry_after_seconds=12.0)),
        ),
        patch("app.clients.teyca.asyncio.sleep", new=AsyncMock()) as sleep_mock,
    ):
        limiter = PostgresCallBudgetLimiter(
            session_factory=session_factory,
            limits=(("minute", 60, 50),),
        )
        with pytest.raises(TeycaRateLimitBusyError) as exc_info:
            await limiter.acquire(max_wait_seconds=5.0)

    sleep_mock.assert_not_awaited()
    session.commit.assert_awaited_once()
    assert exc_info.value.backend == "postgres"
    assert exc_info.value.wait_seconds == pytest.approx(12.0)
    assert exc_info.value.max_wait_seconds == pytest.approx(5.0)


def test_build_teyca_rate_limiter_builds_postgres_limiter() -> None:
    settings = SimpleNamespace(
        teyca_base_url="https://api.example.com/",
        teyca_api_key="api-key",
        teyca_token="token-1",
    )
    session_factory = MagicMock()

    limiter = build_teyca_rate_limiter(cast(Settings, settings), session_factory=session_factory)

    assert isinstance(limiter, PostgresCallBudgetLimiter)


def test_build_teyca_client_passes_explicit_limiter() -> None:
    settings = SimpleNamespace(
        teyca_base_url="https://api.example.com/",
        teyca_api_key="api-key",
        teyca_token="token-1",
    )
    session_factory = MagicMock()

    with patch(
        "app.clients.teyca.build_teyca_rate_limiter", return_value=AsyncMock()
    ) as limiter_mock:
        client = build_teyca_client(cast(Settings, settings), session_factory=session_factory)

    assert isinstance(client, TeycaClient)
    limiter_mock.assert_called_once_with(cast(Settings, settings), session_factory=session_factory)



def test_run_entrypoints_call_asyncio_run() -> None:
    with patch("app.workers.run_consent_sync.asyncio.run") as run_mock:
        run_consent_sync.main()
        run_mock.call_args.args[0].close()
    run_mock.assert_called_once()

    with patch("app.workers.run_listmonk_reconcile.asyncio.run") as run_mock:
        run_listmonk_reconcile.main()
        run_mock.call_args.args[0].close()
    run_mock.assert_called_once()
