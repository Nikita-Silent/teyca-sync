from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from app.api.auth import verify_webhook_token
from app.clients.teyca import BonusOperation, SlidingWindowRateLimiter, TeycaAPIError, TeycaClient
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
from app.mq.publisher import MQPublisher
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


@pytest.mark.asyncio
async def test_mq_publisher_publish_and_route() -> None:
    connection = AsyncMock()
    channel = AsyncMock()
    channel.is_closed = False
    channel.default_exchange = AsyncMock()
    connection.channel.return_value = channel

    publisher = MQPublisher(connection)
    await publisher.publish("queue-x", {"x": 1})

    channel.declare_queue.assert_awaited_once_with("queue-x", durable=True)
    channel.default_exchange.publish.assert_awaited_once()

    with patch.object(publisher, "publish", new_callable=AsyncMock) as publish_mock:
        await publisher.publish_webhook("CREATE", {"a": 1})
        await publisher.publish_webhook("UPDATE", {"a": 1})
        await publisher.publish_webhook("DELETE", {"a": 1})
        with pytest.raises(ValueError):
            await publisher.publish_webhook("OTHER", {"a": 1})

    assert publish_mock.await_count == 3


@pytest.mark.asyncio
async def test_mq_publisher_reopens_closed_channel() -> None:
    connection = AsyncMock()
    closed_channel = AsyncMock()
    closed_channel.is_closed = True
    open_channel = AsyncMock()
    open_channel.is_closed = False
    open_channel.default_exchange = AsyncMock()
    connection.channel.side_effect = [closed_channel, open_channel]

    publisher = MQPublisher(connection)
    publisher._channel = closed_channel

    await publisher.publish("queue-x", {"x": 1})

    assert connection.channel.await_count == 1


@pytest.mark.asyncio
async def test_mq_publisher_declares_queue_once_per_channel() -> None:
    connection = AsyncMock()
    channel = AsyncMock()
    channel.is_closed = False
    channel.default_exchange = AsyncMock()
    connection.channel.return_value = channel

    publisher = MQPublisher(connection)
    await publisher.publish("queue-x", {"x": 1})
    await publisher.publish("queue-x", {"x": 2})

    channel.declare_queue.assert_awaited_once_with("queue-x", durable=True)
    assert channel.default_exchange.publish.await_count == 2


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
    client = TeycaClient(settings=settings, http_client=http_client, rate_limiter=rate_limiter)

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


def test_teyca_client_settings_validation() -> None:
    settings = SimpleNamespace(
        teyca_base_url="https://api.example.com",
        teyca_api_key="",
        teyca_token="",
    )
    client = TeycaClient(settings=settings)
    with pytest.raises(TeycaAPIError):
        client._get_headers()


@pytest.mark.asyncio
async def test_teyca_client_uses_internal_httpx_client_when_not_injected() -> None:
    settings = SimpleNamespace(
        teyca_base_url="https://api.example.com/",
        teyca_api_key="api-key",
        teyca_token="token-1",
    )
    client = TeycaClient(settings=settings, http_client=None)

    httpx_client = AsyncMock()
    httpx_client.post.return_value = SimpleNamespace(status_code=200, text="ok")
    httpx_client.put.return_value = SimpleNamespace(status_code=200, text="ok")
    cm = AsyncMock()
    cm.__aenter__.return_value = httpx_client
    cm.__aexit__.return_value = False

    with patch("app.clients.teyca.httpx.AsyncClient", return_value=cm):
        await client.accrue_bonuses(user_id=1, bonuses=[BonusOperation.one_shot(value="1")])
        await client.update_pass_fields(user_id=1, fields={"key1": "confirmed"})

    assert httpx_client.post.await_count == 1
    assert httpx_client.put.await_count == 1


@pytest.mark.asyncio
async def test_teyca_sliding_window_rate_limiter_waits_when_limit_is_reached() -> None:
    now = 0.0
    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        nonlocal now
        sleep_calls.append(seconds)
        now += seconds

    limiter = SlidingWindowRateLimiter(
        limits=((1.0, 2),),
        clock=lambda: now,
    )

    with patch("app.clients.teyca.asyncio.sleep", side_effect=fake_sleep):
        await limiter.acquire()
        await limiter.acquire()
        await limiter.acquire()

    assert len(sleep_calls) == 1
    assert sleep_calls[0] == pytest.approx(1.0)


def test_run_entrypoints_call_asyncio_run() -> None:
    with patch("app.workers.run_consent_sync.asyncio.run") as run_mock:
        run_consent_sync.main()
        run_mock.call_args.args[0].close()
    run_mock.assert_called_once()

    with patch("app.workers.run_listmonk_reconcile.asyncio.run") as run_mock:
        run_listmonk_reconcile.main()
        run_mock.call_args.args[0].close()
    run_mock.assert_called_once()
