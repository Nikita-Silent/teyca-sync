from __future__ import annotations

import asyncio
import importlib
import logging
import os
import runpy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app import main as app_main
from app.api.webhook import get_mq_publisher
from app.db import session as db_session
from app.logging_config import _normalize_loki_url, configure_logging, shutdown_logging
from app.workers import run_consent_sync, run_listmonk_reconcile
from app.workers import run_queue_consumers
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
async def test_lifespan_testing_branch_sets_mock_publisher() -> None:
    app = SimpleNamespace(state=SimpleNamespace())
    with patch.dict("os.environ", {"TESTING": "1"}, clear=False), patch(
        "app.main.get_settings", return_value=SimpleNamespace(loki_url=None)
    ), patch("app.main.configure_logging"), patch("app.main.shutdown_logging"):
        async with app_main.lifespan(app):
            assert hasattr(app.state, "mq_publisher")


@pytest.mark.asyncio
async def test_lifespan_runtime_branch_connects_and_closes_connection() -> None:
    app = SimpleNamespace(state=SimpleNamespace())
    connection = AsyncMock()
    with patch.dict("os.environ", {}, clear=True), patch(
        "app.main.get_settings", return_value=SimpleNamespace(loki_url="http://loki", rabbitmq_url="amqp://x")
    ), patch("app.main.configure_logging"), patch("app.main.shutdown_logging"), patch(
        "app.main.aio_pika.connect_robust", new=AsyncMock(return_value=connection)
    ):
        async with app_main.lifespan(app):
            assert app.state.mq_publisher is not None
    connection.close.assert_awaited_once()


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
    with patch("app.logging_config.logging_loki.LokiQueueHandler", return_value=loki_queue_handler) as cls_mock:
        configure_logging(loki_url="http://loki", loki_username="user", loki_password="pass")
        cls_mock.assert_called_once()
        kwargs = cls_mock.call_args.kwargs
        assert kwargs["url"] == "http://loki/loki/api/v1/push"
        assert kwargs["version"] == "2"
        assert kwargs["auth"] == ("user", "pass")
        assert kwargs["tags"] == {"service": "teyca-sync"}
    shutdown_logging()
    loki_queue_handler.listener.stop.assert_called()


def test_get_mq_publisher_and_main_guards() -> None:
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(mq_publisher="publisher")))
    assert get_mq_publisher(request) == "publisher"

    worker_dir = Path(__file__).resolve().parents[2] / "app" / "workers"

    def _close_coro(coro: object) -> None:
        if hasattr(coro, "close"):
            coro.close()
        return None

    with patch("asyncio.run", side_effect=_close_coro):
        runpy.run_path(str(worker_dir / "run_consent_sync.py"), run_name="__main__")
    with patch("asyncio.run", side_effect=_close_coro):
        runpy.run_path(str(worker_dir / "run_listmonk_reconcile.py"), run_name="__main__")
    with patch("asyncio.run", side_effect=_close_coro):
        runpy.run_path(str(worker_dir / "run_queue_consumers.py"), run_name="__main__")


@pytest.mark.asyncio
async def test_consumers_runner_core_paths() -> None:
    runner = run_queue_consumers.ConsumersRunner(
        settings=SimpleNamespace(rabbitmq_url="amqp://x"),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
        old_db_repo=AsyncMock(),
    )

    msg = SimpleNamespace(body=b'{"x": 1}')
    assert await runner._parse_payload(msg) == {"x": 1}
    with pytest.raises(ValueError):
        await runner._parse_payload(SimpleNamespace(body=b"not-json"))

    with patch.object(
        run_queue_consumers.ConsumersRunner, "_consume_create", new=AsyncMock()
    ) as create_mock, patch.object(
        run_queue_consumers.ConsumersRunner, "_consume_update", new=AsyncMock()
    ) as update_mock, patch.object(
        run_queue_consumers.ConsumersRunner, "_consume_delete", new=AsyncMock()
    ) as delete_mock:
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
        settings=SimpleNamespace(rabbitmq_url="amqp://x"),
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


@pytest.mark.asyncio
async def test_consumers_runner_consume_commit_and_rollback_paths() -> None:
    runner = run_queue_consumers.ConsumersRunner(
        settings=SimpleNamespace(rabbitmq_url="amqp://x"),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
        old_db_repo=AsyncMock(),
    )
    session = AsyncMock()
    cm = AsyncMock()
    cm.__aenter__.return_value = session
    cm.__aexit__.return_value = False

    with patch("app.workers.run_queue_consumers.SessionLocal", return_value=cm), patch(
        "app.workers.run_queue_consumers.handle_create", new=AsyncMock()
    ), patch("app.workers.run_queue_consumers.UsersRepository"), patch(
        "app.workers.run_queue_consumers.ListmonkUsersRepository"
    ), patch("app.workers.run_queue_consumers.MergeLogRepository"):
        await runner._consume_create({"type": "CREATE", "pass": {"user_id": 1}})
    session.commit.assert_awaited_once()

    session.reset_mock()
    with patch("app.workers.run_queue_consumers.SessionLocal", return_value=cm), patch(
        "app.workers.run_queue_consumers.handle_create", new=AsyncMock(side_effect=RuntimeError("boom"))
    ), patch("app.workers.run_queue_consumers.UsersRepository"), patch(
        "app.workers.run_queue_consumers.ListmonkUsersRepository"
    ), patch("app.workers.run_queue_consumers.MergeLogRepository"):
        with pytest.raises(RuntimeError):
            await runner._consume_create({"type": "CREATE", "pass": {"user_id": 1}})
    session.rollback.assert_awaited_once()

    session.reset_mock()
    with patch("app.workers.run_queue_consumers.SessionLocal", return_value=cm), patch(
        "app.workers.run_queue_consumers.handle_update", new=AsyncMock()
    ), patch("app.workers.run_queue_consumers.UsersRepository"), patch(
        "app.workers.run_queue_consumers.ListmonkUsersRepository"
    ), patch("app.workers.run_queue_consumers.MergeLogRepository"):
        await runner._consume_update({"type": "UPDATE", "pass": {"user_id": 1}})
    session.commit.assert_awaited_once()

    session.reset_mock()
    with patch("app.workers.run_queue_consumers.SessionLocal", return_value=cm), patch(
        "app.workers.run_queue_consumers.handle_update", new=AsyncMock(side_effect=RuntimeError("boom"))
    ), patch("app.workers.run_queue_consumers.UsersRepository"), patch(
        "app.workers.run_queue_consumers.ListmonkUsersRepository"
    ), patch("app.workers.run_queue_consumers.MergeLogRepository"):
        with pytest.raises(RuntimeError):
            await runner._consume_update({"type": "UPDATE", "pass": {"user_id": 1}})
    session.rollback.assert_awaited_once()

    session.reset_mock()
    with patch("app.workers.run_queue_consumers.SessionLocal", return_value=cm), patch(
        "app.workers.run_queue_consumers.handle_delete", new=AsyncMock()
    ), patch("app.workers.run_queue_consumers.UsersRepository"), patch(
        "app.workers.run_queue_consumers.ListmonkUsersRepository"
    ), patch("app.workers.run_queue_consumers.MergeLogRepository"), patch(
        "app.workers.run_queue_consumers.BonusAccrualRepository"
    ):
        await runner._consume_delete({"type": "DELETE", "pass": {"user_id": 1}})
    session.rollback.assert_not_awaited()

    session.reset_mock()
    with patch("app.workers.run_queue_consumers.SessionLocal", return_value=cm), patch(
        "app.workers.run_queue_consumers.handle_delete", new=AsyncMock(side_effect=RuntimeError("boom"))
    ), patch("app.workers.run_queue_consumers.UsersRepository"), patch(
        "app.workers.run_queue_consumers.ListmonkUsersRepository"
    ), patch("app.workers.run_queue_consumers.MergeLogRepository"), patch(
        "app.workers.run_queue_consumers.BonusAccrualRepository"
    ):
        with pytest.raises(RuntimeError):
            await runner._consume_delete({"type": "DELETE", "pass": {"user_id": 1}})
    session.rollback.assert_awaited_once()


@pytest.mark.asyncio
async def test_consumers_runner_run_and_entrypoints() -> None:
    runner = run_queue_consumers.ConsumersRunner(
        settings=SimpleNamespace(rabbitmq_url="amqp://x"),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
        old_db_repo=AsyncMock(),
    )
    queue_obj = AsyncMock()
    channel = AsyncMock()
    channel.declare_queue.return_value = queue_obj
    connection = AsyncMock()
    connection.channel.return_value = channel

    with patch("app.workers.run_queue_consumers.aio_pika.connect_robust", new=AsyncMock(return_value=connection)), patch(
        "app.workers.run_queue_consumers.asyncio.Event"
    ) as event_cls:
        waiter = AsyncMock(side_effect=RuntimeError("stop"))
        event_cls.return_value.wait = waiter
        with pytest.raises(RuntimeError):
            await runner.run()

    runner.old_db_repo.close.assert_awaited_once()
    connection.close.assert_awaited_once()

    with patch("app.workers.run_queue_consumers.get_settings", return_value=SimpleNamespace(export_db_url="db")), patch(
        "app.workers.run_queue_consumers.ListmonkSDKClient"
    ), patch("app.workers.run_queue_consumers.TeycaClient"), patch(
        "app.workers.run_queue_consumers.OldDBRepository"
    ), patch.object(run_queue_consumers.ConsumersRunner, "run", new=AsyncMock()) as run_mock:
        await run_queue_consumers._run()
    run_mock.assert_awaited_once()

    with patch("app.workers.run_queue_consumers.asyncio.run") as run_mock:
        run_queue_consumers.main()
        run_mock.call_args.args[0].close()
    run_mock.assert_called_once()


@pytest.mark.asyncio
async def test_run_single_iteration_workers_log() -> None:
    with patch("app.workers.run_consent_sync.build_consent_sync_worker") as builder, patch(
        "app.workers.run_consent_sync.logger"
    ) as logger:
        worker = AsyncMock()
        worker.run_once.return_value = 2
        builder.return_value = worker
        await run_consent_sync._run()
    logger.info.assert_called_once()

    with patch("app.workers.run_listmonk_reconcile.build_listmonk_reconcile_worker") as builder, patch(
        "app.workers.run_listmonk_reconcile.logger"
    ) as logger:
        worker = AsyncMock()
        worker.run_once.return_value = 3
        builder.return_value = worker
        await run_listmonk_reconcile._run()
    logger.info.assert_called_once()


@pytest.mark.asyncio
async def test_main_uses_webhook_path_from_env() -> None:
    original_webhook = os.environ.get("WEBHOOK")
    original_token = os.environ.get("WEBHOOK_AUTH_TOKEN")
    os.environ["WEBHOOK"] = "/custom-webhook"
    os.environ["WEBHOOK_AUTH_TOKEN"] = "secret-token"
    reloaded_main = importlib.reload(app_main)
    try:
        publisher = AsyncMock()
        reloaded_main.app.dependency_overrides[get_mq_publisher] = lambda: publisher
        payload = {"type": "CREATE", "pass": {"user_id": 1}}
        async with AsyncClient(transport=ASGITransport(app=reloaded_main.app), base_url="http://test") as ac:
            resp = await ac.post(
                "/custom-webhook",
                json=payload,
                headers={"Authorization": "secret-token"},
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
