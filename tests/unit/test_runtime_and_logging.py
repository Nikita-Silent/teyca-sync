from __future__ import annotations

import asyncio
import importlib
import logging
import os
import runpy
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from aio_pika.abc import AbstractIncomingMessage
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient

from app import main as app_main
from app.api.webhook import get_mq_publisher
from app.clients.teyca import TeycaAPIError
from app.config import Settings
from app.db import session as db_session
from app.logging_config import (
    _add_static_fields,
    _normalize_loki_url,
    configure_logging,
    shutdown_logging,
)
from app.repositories.users import UserLockNotAcquiredError
from app.workers import (
    run_consent_sync,
    run_listmonk_reconcile,
    run_queue_consumers,
)


class DummyAwaitableTask:
    def __init__(self) -> None:
        self.cancel = MagicMock()
        self.awaited = False

    def __await__(self):  # type: ignore[override]
        async def _wait() -> None:
            self.awaited = True

        return _wait().__await__()


def _runner_settings(**overrides: object) -> Settings:
    defaults: dict[str, object] = {
        "rabbitmq_url": "amqp://x",
        "rabbitmq_lock_busy_retry_base_delay_ms": 1_000,
        "rabbitmq_lock_busy_retry_max_delay_ms": 30_000,
        "rabbitmq_lock_busy_retry_max_retries": 5,
        "rabbitmq_teyca_rate_limit_retry_base_delay_ms": 60_000,
        "rabbitmq_teyca_rate_limit_retry_max_delay_ms": 15 * 60_000,
        "rabbitmq_teyca_rate_limit_retry_max_retries": 10,
    }
    defaults.update(overrides)
    return cast(Settings, SimpleNamespace(**defaults))


@pytest.mark.asyncio
async def test_lifespan_testing_branch_sets_mock_publisher() -> None:
    app = cast(FastAPI, SimpleNamespace(state=SimpleNamespace()))
    with (
        patch.dict("os.environ", {"TESTING": "1"}, clear=False),
        patch("app.main.get_settings", return_value=SimpleNamespace(loki_url=None)),
        patch("app.main.configure_logging"),
        patch("app.main.shutdown_logging"),
        patch("app.main.write_heartbeat", new=AsyncMock()),
    ):
        async with app_main.lifespan(app):
            assert hasattr(app.state, "mq_publisher")


@pytest.mark.asyncio
async def test_lifespan_runtime_branch_connects_and_closes_connection() -> None:
    app = cast(FastAPI, SimpleNamespace(state=SimpleNamespace()))
    connection = AsyncMock()
    with (
        patch.dict("os.environ", {}, clear=True),
        patch(
            "app.main.get_settings",
            return_value=SimpleNamespace(loki_url="http://loki", rabbitmq_url="amqp://x"),
        ),
        patch("app.main.configure_logging"),
        patch("app.main.shutdown_logging"),
        patch("app.main.aio_pika.connect_robust", new=AsyncMock(return_value=connection)),
        patch("app.main._start_heartbeat_task") as heartbeat_task_mock,
    ):
        heartbeat_task = DummyAwaitableTask()
        heartbeat_task_mock.return_value = heartbeat_task
        async with app_main.lifespan(app):
            assert app.state.mq_publisher is not None
    connection.close.assert_awaited_once()
    heartbeat_task.cancel.assert_called_once()
    assert heartbeat_task.awaited is True


@pytest.mark.asyncio
async def test_lifespan_always_shuts_logging_on_error() -> None:
    app = cast(FastAPI, SimpleNamespace(state=SimpleNamespace()))
    with (
        patch.dict("os.environ", {"TESTING": "1"}, clear=False),
        patch("app.main.get_settings", return_value=SimpleNamespace(loki_url=None)),
        patch("app.main.configure_logging"),
        patch("app.main.shutdown_logging") as shutdown_mock,
        patch("app.main.write_heartbeat", new=AsyncMock()),
    ):
        with pytest.raises(RuntimeError, match="boom"):
            async with app_main.lifespan(app):
                raise RuntimeError("boom")

    shutdown_mock.assert_called_once()


@pytest.mark.asyncio
async def test_get_session_yields_session() -> None:
    fake_session = AsyncMock()
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = fake_session
    context_manager.__aexit__.return_value = False
    with patch("app.db.session.SessionLocal", return_value=context_manager):
        gen = db_session.get_session()
        yielded = await anext(gen)
        assert yielded is fake_session
        with pytest.raises(StopAsyncIteration):
            await anext(gen)


def test_loki_handler_and_logging_config() -> None:
    assert _normalize_loki_url("http://loki") == "http://loki/loki/api/v1/push"
    assert _normalize_loki_url("http://loki/loki/api/v1/push") == "http://loki/loki/api/v1/push"

    with pytest.raises(RuntimeError):
        configure_logging(loki_url=None)

    loki_queue_handler = MagicMock()
    loki_queue_handler.listener = MagicMock()
    loki_queue_handler.level = logging.INFO
    with patch(
        "app.logging_config.logging_loki.LokiQueueHandler", return_value=loki_queue_handler
    ) as cls_mock:
        configure_logging(loki_url="http://loki", loki_username="user", loki_password="pass")
        cls_mock.assert_called_once()
        kwargs = cls_mock.call_args.kwargs
        assert kwargs["url"] == "http://loki/loki/api/v1/push"
        assert kwargs["version"] == "2"
        assert kwargs["auth"] == ("user", "pass")
        assert kwargs["tags"] == {"service": "teyca-sync", "component": "app"}
    shutdown_logging()
    loki_queue_handler.listener.stop.assert_called()


def test_add_static_fields_processor() -> None:
    processor = _add_static_fields(service_name="svc", component="app")

    assert processor(None, "info", {"event": "x"}) == {
        "event": "x",
        "service": "svc",
        "component": "app",
    }
    assert processor(None, "info", {"service": "other", "component": "worker"}) == {
        "service": "other",
        "component": "worker",
    }


def test_get_mq_publisher_and_main_guards() -> None:
    request = cast(
        Request[Any],
        SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(mq_publisher="publisher"))),
    )
    assert get_mq_publisher(request) == "publisher"

    worker_dir = Path(__file__).resolve().parents[2] / "app" / "workers"

    def _close_coro(coro: object) -> None:
        closeable = cast(Any, coro)
        if hasattr(closeable, "close"):
            closeable.close()
        return None

    with patch("asyncio.run", side_effect=_close_coro):
        runpy.run_path(str(worker_dir / "run_consent_sync.py"), run_name="__main__")
    with patch("asyncio.run", side_effect=_close_coro):
        runpy.run_path(
            str(worker_dir / "run_listmonk_duplicate_subscriber.py"), run_name="__main__"
        )
    with patch("asyncio.run", side_effect=_close_coro):
        runpy.run_path(str(worker_dir / "run_listmonk_reconcile.py"), run_name="__main__")
    with patch("asyncio.run", side_effect=_close_coro):
        runpy.run_path(str(worker_dir / "run_queue_consumers.py"), run_name="__main__")


@pytest.mark.asyncio
async def test_consumers_runner_core_paths() -> None:
    runner = run_queue_consumers.ConsumersRunner(
        settings=_runner_settings(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
        old_db_repo=AsyncMock(),
    )

    msg = cast(AbstractIncomingMessage, SimpleNamespace(body=b'{"x": 1}'))
    assert await runner._parse_payload(msg) == {"x": 1}
    with pytest.raises(ValueError):
        await runner._parse_payload(
            cast(AbstractIncomingMessage, SimpleNamespace(body=b"not-json"))
        )

    with (
        patch.object(
            run_queue_consumers.ConsumersRunner, "_consume_create", new=AsyncMock()
        ) as create_mock,
        patch.object(
            run_queue_consumers.ConsumersRunner, "_consume_update", new=AsyncMock()
        ) as update_mock,
        patch.object(
            run_queue_consumers.ConsumersRunner, "_consume_delete", new=AsyncMock()
        ) as delete_mock,
    ):
        await runner._process(msg, run_queue_consumers.QUEUE_CREATE)
        await runner._process(msg, run_queue_consumers.QUEUE_UPDATE)
        await runner._process(msg, run_queue_consumers.QUEUE_DELETE)
        with pytest.raises(ValueError):
            await runner._process(msg, "unknown")

    assert create_mock.await_count == 1
    assert update_mock.await_count == 1
    assert delete_mock.await_count == 1


@pytest.mark.asyncio
async def test_consumers_runner_callback_ack_and_reject() -> None:
    runner = run_queue_consumers.ConsumersRunner(
        settings=_runner_settings(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
        old_db_repo=AsyncMock(),
    )

    message = AsyncMock()
    with patch.object(run_queue_consumers.ConsumersRunner, "_process", new=AsyncMock()):
        await runner._callback(message, run_queue_consumers.QUEUE_CREATE)
    message.ack.assert_awaited_once()

    message = AsyncMock()
    with patch.object(
        run_queue_consumers.ConsumersRunner,
        "_process",
        new=AsyncMock(side_effect=RuntimeError("boom")),
    ):
        await runner._callback(message, run_queue_consumers.QUEUE_CREATE)
    message.reject.assert_awaited_once_with(requeue=True)

    message = AsyncMock()
    message.headers = {}
    message.body = b'{"type":"UPDATE"}'
    message.message_id = "msg-1"
    message.correlation_id = "corr-1"
    message.delivery_tag = 101
    message.content_type = "application/json"
    message.content_encoding = None
    message.timestamp = None
    message.type = None
    message.app_id = None
    channel = AsyncMock()
    channel.default_exchange = AsyncMock()
    runner._channel = channel
    with (
        patch.object(
            run_queue_consumers.ConsumersRunner,
            "_process",
            new=AsyncMock(side_effect=UserLockNotAcquiredError(user_id=42)),
        ),
        patch("app.workers.run_queue_consumers.logger") as logger,
    ):
        await runner._callback(message, run_queue_consumers.QUEUE_UPDATE)
    message.ack.assert_awaited_once()
    message.reject.assert_not_awaited()
    logger.warning.assert_called_once()
    warning_kwargs = logger.warning.call_args.kwargs
    assert warning_kwargs["result"] == "user_lock_busy"
    assert warning_kwargs["user_id"] == 42
    assert warning_kwargs["queue_name"] == run_queue_consumers.QUEUE_UPDATE
    published_message = channel.default_exchange.publish.await_args.args[0]
    assert published_message.headers["x-lock-busy-retry-count"] == 1
    assert published_message.headers["x-original-queue"] == run_queue_consumers.QUEUE_UPDATE
    assert published_message.expiration == timedelta(seconds=1)
    assert channel.default_exchange.publish.await_args.kwargs["routing_key"] == "queue-update-retry"


@pytest.mark.asyncio
async def test_consumers_runner_callback_dead_letters_after_lock_retry_limit() -> None:
    runner = run_queue_consumers.ConsumersRunner(
        settings=_runner_settings(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
        old_db_repo=AsyncMock(),
    )

    message = AsyncMock()
    message.headers = {"x-lock-busy-retry-count": 5}
    message.body = b'{"type":"UPDATE"}'
    message.message_id = "msg-2"
    message.correlation_id = "corr-2"
    message.delivery_tag = 102
    message.content_type = "application/json"
    message.content_encoding = None
    message.timestamp = None
    message.type = None
    message.app_id = None
    channel = AsyncMock()
    channel.default_exchange = AsyncMock()
    runner._channel = channel

    with (
        patch.object(
            run_queue_consumers.ConsumersRunner,
            "_process",
            new=AsyncMock(side_effect=UserLockNotAcquiredError(user_id=77)),
        ),
        patch("app.workers.run_queue_consumers.logger") as logger,
    ):
        await runner._callback(message, run_queue_consumers.QUEUE_UPDATE)

    message.ack.assert_awaited_once()
    message.reject.assert_not_awaited()
    logger.warning.assert_called_once()
    warning_kwargs = logger.warning.call_args.kwargs
    assert warning_kwargs["result"] == "user_lock_busy_dead_lettered"
    assert warning_kwargs["retry_count"] == 6
    assert warning_kwargs["retry_queue_name"] == "queue-update-dead"
    published_message = channel.default_exchange.publish.await_args.args[0]
    assert published_message.headers["x-lock-busy-retry-count"] == 6
    assert published_message.expiration is None
    assert channel.default_exchange.publish.await_args.kwargs["routing_key"] == "queue-update-dead"


@pytest.mark.asyncio
async def test_consumers_runner_callback_requeues_teyca_rate_limit_with_delay() -> None:
    runner = run_queue_consumers.ConsumersRunner(
        settings=_runner_settings(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
        old_db_repo=AsyncMock(),
    )

    message = AsyncMock()
    message.headers = {}
    message.body = b'{"type":"UPDATE","pass":{"user_id":5771594}}'
    message.message_id = "msg-429"
    message.correlation_id = "corr-429"
    message.delivery_tag = 201
    message.content_type = "application/json"
    message.content_encoding = None
    message.timestamp = None
    message.type = None
    message.app_id = None
    channel = AsyncMock()
    channel.default_exchange = AsyncMock()
    runner._channel = channel

    with (
        patch.object(
            run_queue_consumers.ConsumersRunner,
            "_process",
            new=AsyncMock(
                side_effect=TeycaAPIError(
                    "Teyca bonuses request failed: status=429, body=Retry later",
                    status_code=429,
                )
            ),
        ),
        patch("app.workers.run_queue_consumers.logger") as logger,
    ):
        await runner._callback(message, run_queue_consumers.QUEUE_UPDATE)

    message.ack.assert_awaited_once()
    message.reject.assert_not_awaited()
    logger.warning.assert_called_once()
    warning_kwargs = logger.warning.call_args.kwargs
    assert warning_kwargs["result"] == "teyca_rate_limited"
    assert warning_kwargs["retry_count"] == 1
    assert warning_kwargs["retry_queue_name"] == "queue-update-retry"
    assert warning_kwargs["status_code"] == 429
    published_message = channel.default_exchange.publish.await_args.args[0]
    assert published_message.headers["x-teyca-rate-limit-retry-count"] == 1
    assert published_message.headers["x-original-queue"] == run_queue_consumers.QUEUE_UPDATE
    assert published_message.expiration == timedelta(minutes=1)
    assert channel.default_exchange.publish.await_args.kwargs["routing_key"] == "queue-update-retry"


@pytest.mark.asyncio
async def test_consumers_runner_callback_dead_letters_teyca_rate_limit_after_limit() -> None:
    runner = run_queue_consumers.ConsumersRunner(
        settings=_runner_settings(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
        old_db_repo=AsyncMock(),
    )

    message = AsyncMock()
    message.headers = {"x-teyca-rate-limit-retry-count": 10}
    message.body = b'{"type":"CREATE","pass":{"user_id":42}}'
    message.message_id = "msg-429-dead"
    message.correlation_id = "corr-429-dead"
    message.delivery_tag = 202
    message.content_type = "application/json"
    message.content_encoding = None
    message.timestamp = None
    message.type = None
    message.app_id = None
    channel = AsyncMock()
    channel.default_exchange = AsyncMock()
    runner._channel = channel

    with (
        patch.object(
            run_queue_consumers.ConsumersRunner,
            "_process",
            new=AsyncMock(
                side_effect=TeycaAPIError(
                    "Teyca pass update failed: status=429, body=Retry later",
                    status_code=429,
                )
            ),
        ),
        patch("app.workers.run_queue_consumers.logger") as logger,
    ):
        await runner._callback(message, run_queue_consumers.QUEUE_CREATE)

    message.ack.assert_awaited_once()
    message.reject.assert_not_awaited()
    logger.warning.assert_called_once()
    warning_kwargs = logger.warning.call_args.kwargs
    assert warning_kwargs["result"] == "teyca_rate_limited_dead_lettered"
    assert warning_kwargs["retry_count"] == 11
    assert warning_kwargs["retry_queue_name"] == "queue-create-dead"
    published_message = channel.default_exchange.publish.await_args.args[0]
    assert published_message.headers["x-teyca-rate-limit-retry-count"] == 11
    assert published_message.expiration is None
    assert channel.default_exchange.publish.await_args.kwargs["routing_key"] == "queue-create-dead"


@pytest.mark.asyncio
async def test_consumers_runner_callback_uses_configured_retry_limits() -> None:
    runner = run_queue_consumers.ConsumersRunner(
        settings=_runner_settings(
            rabbitmq_lock_busy_retry_base_delay_ms=2_000,
            rabbitmq_lock_busy_retry_max_delay_ms=5_000,
            rabbitmq_lock_busy_retry_max_retries=2,
            rabbitmq_teyca_rate_limit_retry_base_delay_ms=5_000,
            rabbitmq_teyca_rate_limit_retry_max_delay_ms=12_000,
            rabbitmq_teyca_rate_limit_retry_max_retries=1,
        ),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
        old_db_repo=AsyncMock(),
    )

    lock_message = AsyncMock()
    lock_message.headers = {}
    lock_message.body = b'{"type":"UPDATE","pass":{"user_id":55}}'
    lock_message.content_type = "application/json"
    lock_message.content_encoding = None
    lock_message.timestamp = None
    lock_message.type = None
    lock_message.app_id = None
    rate_limit_message = AsyncMock()
    rate_limit_message.headers = {"x-teyca-rate-limit-retry-count": 1}
    rate_limit_message.body = b'{"type":"CREATE","pass":{"user_id":56}}'
    rate_limit_message.content_type = "application/json"
    rate_limit_message.content_encoding = None
    rate_limit_message.timestamp = None
    rate_limit_message.type = None
    rate_limit_message.app_id = None
    channel = AsyncMock()
    channel.default_exchange = AsyncMock()
    runner._channel = channel

    with patch.object(
        run_queue_consumers.ConsumersRunner,
        "_process",
        new=AsyncMock(side_effect=UserLockNotAcquiredError(user_id=55)),
    ):
        await runner._callback(lock_message, run_queue_consumers.QUEUE_UPDATE)

    lock_published_message = channel.default_exchange.publish.await_args_list[0].args[0]
    assert lock_published_message.expiration == timedelta(seconds=2)
    assert channel.default_exchange.publish.await_args_list[0].kwargs["routing_key"] == (
        "queue-update-retry"
    )

    with patch.object(
        run_queue_consumers.ConsumersRunner,
        "_process",
        new=AsyncMock(
            side_effect=TeycaAPIError(
                "Teyca pass update failed: status=429, body=Retry later",
                status_code=429,
            )
        ),
    ):
        await runner._callback(rate_limit_message, run_queue_consumers.QUEUE_CREATE)

    rate_limit_published_message = channel.default_exchange.publish.await_args_list[1].args[0]
    assert rate_limit_published_message.expiration is None
    assert channel.default_exchange.publish.await_args_list[1].kwargs["routing_key"] == (
        "queue-create-dead"
    )


@pytest.mark.asyncio
async def test_consumers_runner_process_waits_for_lock_after_retry_header() -> None:
    runner = run_queue_consumers.ConsumersRunner(
        settings=_runner_settings(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
        old_db_repo=AsyncMock(),
    )
    message = AsyncMock()
    message.body = b'{"type":"UPDATE","pass":{"user_id":42}}'
    message.headers = {run_queue_consumers.LOCK_BUSY_RETRY_HEADER: 1}

    with (
        patch.object(
            run_queue_consumers.ConsumersRunner, "_consume_create", new=AsyncMock()
        ) as consume_create,
        patch.object(
            run_queue_consumers.ConsumersRunner, "_consume_update", new=AsyncMock()
        ) as consume_update,
        patch.object(
            run_queue_consumers.ConsumersRunner, "_consume_delete", new=AsyncMock()
        ) as consume_delete,
    ):
        await runner._process(message, run_queue_consumers.QUEUE_UPDATE)

    consume_create.assert_not_awaited()
    consume_delete.assert_not_awaited()
    consume_update.assert_awaited_once_with(
        {"type": "UPDATE", "pass": {"user_id": 42}},
        wait_for_lock=True,
    )


@pytest.mark.asyncio
async def test_consumers_runner_callback_respects_semaphore_limit() -> None:
    runner = run_queue_consumers.ConsumersRunner(
        settings=_runner_settings(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
        old_db_repo=AsyncMock(),
        _process_semaphore=asyncio.Semaphore(1),
    )

    active = 0
    max_active = 0
    first_started = asyncio.Event()
    second_started = asyncio.Event()
    release = asyncio.Event()
    started_count = 0

    async def process_side_effect(*_: object, **__: object) -> None:
        nonlocal active, max_active, started_count
        active += 1
        max_active = max(max_active, active)
        started_count += 1
        if started_count == 1:
            first_started.set()
        else:
            second_started.set()
        await release.wait()
        active -= 1

    msg1 = AsyncMock()
    msg2 = AsyncMock()
    with patch.object(run_queue_consumers.ConsumersRunner, "_process", new=process_side_effect):
        task1 = asyncio.create_task(runner._callback(msg1, run_queue_consumers.QUEUE_CREATE))
        task2 = asyncio.create_task(runner._callback(msg2, run_queue_consumers.QUEUE_CREATE))
        await first_started.wait()
        await asyncio.sleep(0)
        assert second_started.is_set() is False
        release.set()
        await asyncio.gather(task1, task2)

    assert max_active == 1
    assert second_started.is_set() is True
    msg1.ack.assert_awaited_once()
    msg2.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_consumers_runner_consume_commit_and_rollback_paths() -> None:
    runner = run_queue_consumers.ConsumersRunner(
        settings=_runner_settings(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
        old_db_repo=AsyncMock(),
    )
    session = AsyncMock()
    cm = AsyncMock()
    cm.__aenter__.return_value = session
    cm.__aexit__.return_value = False

    with (
        patch("app.workers.run_queue_consumers.SessionLocal", return_value=cm),
        patch("app.workers.run_queue_consumers.handle_create", new=AsyncMock()),
        patch("app.workers.run_queue_consumers.UsersRepository"),
        patch("app.workers.run_queue_consumers.ListmonkUsersRepository"),
        patch("app.workers.run_queue_consumers.MergeLogRepository"),
    ):
        await runner._consume_create({"type": "CREATE", "pass": {"user_id": 1}})
    session.commit.assert_awaited_once()

    session.reset_mock()
    with (
        patch("app.workers.run_queue_consumers.SessionLocal", return_value=cm),
        patch(
            "app.workers.run_queue_consumers.handle_create",
            new=AsyncMock(side_effect=RuntimeError("boom")),
        ),
        patch("app.workers.run_queue_consumers.UsersRepository"),
        patch("app.workers.run_queue_consumers.ListmonkUsersRepository"),
        patch("app.workers.run_queue_consumers.MergeLogRepository"),
    ):
        with pytest.raises(RuntimeError):
            await runner._consume_create({"type": "CREATE", "pass": {"user_id": 1}})
    session.rollback.assert_awaited_once()

    session.reset_mock()
    with (
        patch("app.workers.run_queue_consumers.SessionLocal", return_value=cm),
        patch("app.workers.run_queue_consumers.handle_update", new=AsyncMock()),
        patch("app.workers.run_queue_consumers.UsersRepository"),
        patch("app.workers.run_queue_consumers.ListmonkUsersRepository"),
        patch("app.workers.run_queue_consumers.MergeLogRepository"),
    ):
        await runner._consume_update({"type": "UPDATE", "pass": {"user_id": 1}})
    session.commit.assert_awaited_once()

    session.reset_mock()
    with (
        patch("app.workers.run_queue_consumers.SessionLocal", return_value=cm),
        patch(
            "app.workers.run_queue_consumers.handle_update",
            new=AsyncMock(side_effect=RuntimeError("boom")),
        ),
        patch("app.workers.run_queue_consumers.UsersRepository"),
        patch("app.workers.run_queue_consumers.ListmonkUsersRepository"),
        patch("app.workers.run_queue_consumers.MergeLogRepository"),
    ):
        with pytest.raises(RuntimeError):
            await runner._consume_update({"type": "UPDATE", "pass": {"user_id": 1}})
    session.rollback.assert_awaited_once()

    session.reset_mock()
    with (
        patch("app.workers.run_queue_consumers.SessionLocal", return_value=cm),
        patch("app.workers.run_queue_consumers.handle_delete", new=AsyncMock()),
        patch("app.workers.run_queue_consumers.UsersRepository"),
        patch("app.workers.run_queue_consumers.ListmonkUsersRepository"),
        patch("app.workers.run_queue_consumers.MergeLogRepository"),
        patch("app.workers.run_queue_consumers.BonusAccrualRepository"),
    ):
        await runner._consume_delete({"type": "DELETE", "pass": {"user_id": 1}})
    session.rollback.assert_not_awaited()

    session.reset_mock()
    with (
        patch("app.workers.run_queue_consumers.SessionLocal", return_value=cm),
        patch(
            "app.workers.run_queue_consumers.handle_delete",
            new=AsyncMock(side_effect=RuntimeError("boom")),
        ),
        patch("app.workers.run_queue_consumers.UsersRepository"),
        patch("app.workers.run_queue_consumers.ListmonkUsersRepository"),
        patch("app.workers.run_queue_consumers.MergeLogRepository"),
        patch("app.workers.run_queue_consumers.BonusAccrualRepository"),
    ):
        with pytest.raises(RuntimeError):
            await runner._consume_delete({"type": "DELETE", "pass": {"user_id": 1}})
    session.rollback.assert_awaited_once()


@pytest.mark.asyncio
async def test_consumers_runner_run_and_entrypoints() -> None:
    runner = run_queue_consumers.ConsumersRunner(
        settings=_runner_settings(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
        old_db_repo=AsyncMock(),
    )
    queue_obj = AsyncMock()
    channel = AsyncMock()
    channel.declare_queue.return_value = queue_obj
    connection = AsyncMock()
    connection.channel.return_value = channel
    heartbeat_task = DummyAwaitableTask()

    with (
        patch(
            "app.workers.run_queue_consumers.aio_pika.connect_robust",
            new=AsyncMock(return_value=connection),
        ),
        patch("app.workers.run_queue_consumers.asyncio.Event") as event_cls,
        patch(
            "app.workers.run_queue_consumers._start_heartbeat_task",
            return_value=heartbeat_task,
        ),
    ):
        waiter = AsyncMock(side_effect=RuntimeError("stop"))
        event_cls.return_value.wait = waiter
        with pytest.raises(RuntimeError):
            await runner.run()

    old_db_repo = cast(AsyncMock, runner.old_db_repo)
    old_db_repo.close.assert_awaited_once()
    connection.close.assert_awaited_once()
    heartbeat_task.cancel.assert_called_once()
    assert heartbeat_task.awaited is True

    with (
        patch(
            "app.workers.run_queue_consumers.get_settings",
            return_value=_runner_settings(
                export_db_url="db",
                export_db_request_timeout_seconds=12.5,
            ),
        ),
        patch("app.workers.run_queue_consumers.ListmonkSDKClient"),
        patch("app.workers.run_queue_consumers.build_teyca_client"),
        patch("app.workers.run_queue_consumers.OldDBRepository") as old_db_repo_cls,
        patch("app.workers.run_queue_consumers.configure_logging"),
        patch("app.workers.run_queue_consumers.shutdown_logging"),
        patch.object(run_queue_consumers.ConsumersRunner, "run", new=AsyncMock()) as run_mock,
    ):
        await run_queue_consumers._run()
    run_mock.assert_awaited_once()
    old_db_repo_cls.assert_called_once_with("db", request_timeout_seconds=12.5)

    with patch("app.workers.run_queue_consumers.asyncio.run") as run_mock:
        run_queue_consumers.main()
        run_mock.call_args.args[0].close()
    run_mock.assert_called_once()


@pytest.mark.asyncio
async def test_consumers_runner_run_clamps_concurrency_to_db_capacity() -> None:
    runner = run_queue_consumers.ConsumersRunner(
        settings=_runner_settings(
            rabbitmq_consumer_prefetch_count=10,
            rabbitmq_consumer_max_concurrency=9,
            database_pool_size=2,
            database_pool_max_overflow=0,
        ),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
        old_db_repo=AsyncMock(),
    )
    queue_obj = AsyncMock()
    channel = AsyncMock()
    channel.declare_queue.return_value = queue_obj
    connection = AsyncMock()
    connection.channel.return_value = channel
    heartbeat_task = DummyAwaitableTask()

    with (
        patch(
            "app.workers.run_queue_consumers.aio_pika.connect_robust",
            new=AsyncMock(return_value=connection),
        ),
        patch("app.workers.run_queue_consumers.asyncio.Event") as event_cls,
        patch(
            "app.workers.run_queue_consumers._start_heartbeat_task",
            return_value=heartbeat_task,
        ),
        patch("app.workers.run_queue_consumers.logger") as logger,
    ):
        waiter = AsyncMock(side_effect=RuntimeError("stop"))
        event_cls.return_value.wait = waiter
        with pytest.raises(RuntimeError):
            await runner.run()

    logger.info.assert_called_once()
    kwargs = logger.info.call_args.kwargs
    assert kwargs["prefetch_count"] == 10
    assert kwargs["db_capacity"] == 2
    assert kwargs["max_concurrency"] == 2


@pytest.mark.asyncio
async def test_app_heartbeat_task_logs_and_survives_write_failure() -> None:
    sleep_calls = 0

    async def fake_sleep(_: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        raise asyncio.CancelledError()

    task = None
    with (
        patch("app.main.write_heartbeat", new=AsyncMock(side_effect=RuntimeError("boom"))),
        patch("app.main.logger") as logger,
        patch("app.main.asyncio.sleep", side_effect=fake_sleep),
    ):
        task = app_main._start_heartbeat_task("app", interval_seconds=15)
        with pytest.raises(asyncio.CancelledError):
            await task

    assert task is not None
    assert sleep_calls == 1
    logger.error.assert_called_once()


@pytest.mark.asyncio
async def test_run_single_iteration_workers_log() -> None:
    with (
        patch("app.workers.run_consent_sync.build_consent_sync_worker") as builder,
        patch("app.workers.run_consent_sync.logger") as logger,
        patch("app.workers.run_consent_sync.write_heartbeat", new=AsyncMock()) as heartbeat_mock,
    ):
        worker = AsyncMock()
        worker.run_once.return_value = 2
        builder.return_value = worker
        await run_consent_sync._run()
    logger.info.assert_called_once()
    assert heartbeat_mock.await_count == 3

    with (
        patch("app.workers.run_listmonk_reconcile.build_listmonk_reconcile_worker") as builder,
        patch("app.workers.run_listmonk_reconcile.logger") as logger,
        patch(
            "app.workers.run_listmonk_reconcile.write_heartbeat", new=AsyncMock()
        ) as heartbeat_mock,
    ):
        worker = AsyncMock()
        worker.run_once.return_value = 3
        builder.return_value = worker
        await run_listmonk_reconcile._run()
    logger.info.assert_called_once()
    assert heartbeat_mock.await_count == 3


@pytest.mark.asyncio
async def test_worker_heartbeat_failures_are_best_effort() -> None:
    heartbeat_mock = AsyncMock(side_effect=[RuntimeError("boom"), None, RuntimeError("boom")])
    with (
        patch("app.workers.run_consent_sync.build_consent_sync_worker") as builder,
        patch("app.workers.run_consent_sync.logger") as logger,
        patch("app.workers.run_consent_sync.write_heartbeat", new=heartbeat_mock),
    ):
        worker = AsyncMock()
        worker.run_once.return_value = 2
        builder.return_value = worker
        await run_consent_sync._run()
    logger.warning.assert_called()
    logger.info.assert_called_once()

    heartbeat_mock = AsyncMock(side_effect=[RuntimeError("boom"), None, RuntimeError("boom")])
    with (
        patch("app.workers.run_listmonk_reconcile.build_listmonk_reconcile_worker") as builder,
        patch("app.workers.run_listmonk_reconcile.logger") as logger,
        patch("app.workers.run_listmonk_reconcile.write_heartbeat", new=heartbeat_mock),
    ):
        worker = AsyncMock()
        worker.run_once.return_value = 3
        builder.return_value = worker
        await run_listmonk_reconcile._run()
    logger.error.assert_called()
    logger.info.assert_called_once()


@pytest.mark.asyncio
async def test_single_iteration_workers_handle_listmonk_transient_errors() -> None:
    with (
        patch("app.workers.run_consent_sync.build_consent_sync_worker") as builder,
        patch("app.workers.run_consent_sync.logger") as logger,
        patch("app.workers.run_consent_sync.write_heartbeat", new=AsyncMock()) as heartbeat_mock,
    ):
        worker = AsyncMock()
        worker.run_once.side_effect = httpx.ReadTimeout("timed out")
        builder.return_value = worker
        await run_consent_sync._run()
    logger.error.assert_called_once()
    assert heartbeat_mock.await_count == 3

    with (
        patch("app.workers.run_listmonk_reconcile.build_listmonk_reconcile_worker") as builder,
        patch("app.workers.run_listmonk_reconcile.logger") as logger,
        patch(
            "app.workers.run_listmonk_reconcile.write_heartbeat", new=AsyncMock()
        ) as heartbeat_mock,
    ):
        worker = AsyncMock()
        worker.run_once.side_effect = httpx.ReadTimeout("timed out")
        builder.return_value = worker
        with pytest.raises(httpx.ReadTimeout):
            await run_listmonk_reconcile._run()
    logger.error.assert_called_once()
    assert heartbeat_mock.await_count == 3


@pytest.mark.asyncio
async def test_main_uses_webhook_path_from_env() -> None:
    original_webhook = os.environ.get("WEBHOOK")
    original_token = os.environ.get("WEBHOOK_AUTH_TOKEN")
    os.environ["WEBHOOK"] = "/custom-webhook"
    os.environ["WEBHOOK_AUTH_TOKEN"] = "secret-token"
    reloaded_main = importlib.reload(app_main)
    try:
        publisher = AsyncMock()

        async def override_publisher() -> AsyncMock:
            return publisher

        reloaded_main.app.dependency_overrides[get_mq_publisher] = override_publisher
        payload = {"type": "CREATE", "pass": {"user_id": 1}}
        async with AsyncClient(
            transport=ASGITransport(app=reloaded_main.app), base_url="http://test"
        ) as ac:
            resp = await asyncio.wait_for(
                ac.post(
                    "/custom-webhook",
                    json=payload,
                    headers={"Authorization": "secret-token"},
                ),
                timeout=5,
            )
        assert resp.status_code == 200
        publisher.publish_webhook.assert_called_once()
    finally:
        reloaded_main.app.dependency_overrides.pop(get_mq_publisher, None)
        if original_webhook is None:
            os.environ.pop("WEBHOOK", None)
        else:
            os.environ["WEBHOOK"] = original_webhook
        if original_token is None:
            os.environ.pop("WEBHOOK_AUTH_TOKEN", None)
        else:
            os.environ["WEBHOOK_AUTH_TOKEN"] = original_token
        importlib.reload(app_main)
