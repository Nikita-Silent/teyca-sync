from __future__ import annotations

import asyncio
import importlib
import logging
import os
import runpy
from pathlib import Path
from queue import Queue
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import requests
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app import main as app_main
from app.db import session as db_session
from app.logging_config import (
    _add_static_fields,
    _normalize_loki_url,
    configure_logging,
    shutdown_logging,
)
from app.workers import run_consent_sync, run_listmonk_reconcile


class DummyAwaitableTask:
    def __init__(self) -> None:
        self.cancel = MagicMock()
        self.awaited = False

    def __await__(self):  # type: ignore[override]
        async def _wait() -> None:
            self.awaited = True

        return _wait().__await__()


@pytest.mark.asyncio
async def test_lifespan_starts_and_cancels_heartbeat() -> None:
    app = FastAPI()
    with (
        patch("app.main.get_settings", return_value=SimpleNamespace(loki_url=None)),
        patch("app.main.configure_logging"),
        patch("app.main.shutdown_logging"),
        patch("app.main.write_heartbeat", new=AsyncMock()) as heartbeat_mock,
        patch("app.main._start_heartbeat_task") as heartbeat_task_mock,
    ):
        heartbeat_task = DummyAwaitableTask()
        heartbeat_task_mock.return_value = heartbeat_task
        async with app_main.lifespan(app):
            pass
    heartbeat_mock.assert_awaited_once()
    heartbeat_task.cancel.assert_called_once()
    assert heartbeat_task.awaited is True


@pytest.mark.asyncio
async def test_lifespan_always_shuts_logging_on_error() -> None:
    app = FastAPI()
    with (
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

    session = requests.Session()
    original_request = MagicMock(return_value=object())
    session.request = original_request  # type: ignore[method-assign]
    loki_queue_handler = MagicMock()
    loki_queue_handler.listener = MagicMock()
    loki_queue_handler.level = logging.INFO
    loki_queue_handler.queue = Queue()
    loki_queue_handler.queue.put("pending-log")
    emitter_close = MagicMock()
    loki_queue_handler.handler = SimpleNamespace(
        emitter=SimpleNamespace(session=session, close=emitter_close)
    )
    with patch(
        "app.logging_config.logging_loki.LokiQueueHandler", return_value=loki_queue_handler
    ) as cls_mock:
        configure_logging(
            loki_url="http://loki",
            loki_username="user",
            loki_password="pass",
            loki_request_timeout_seconds=7.5,
        )
        cls_mock.assert_called_once()
        kwargs = cls_mock.call_args.kwargs
        assert kwargs["url"] == "http://loki/loki/api/v1/push"
        assert kwargs["version"] == "2"
        assert kwargs["auth"] == ("user", "pass")
        assert kwargs["tags"] == {"service": "teyca-sync", "component": "app"}
        session.request("POST", "http://loki")
        original_request.assert_called_once_with("POST", "http://loki", timeout=7.5)
        with patch("logging.Handler.handleError") as base_handle_error:
            loki_queue_handler.handler.handleError(
                logging.LogRecord("x", logging.INFO, "", 1, "x", (), None)
            )
            emitter_close.assert_called_once()
            base_handle_error.assert_not_called()
    shutdown_logging()
    loki_queue_handler.listener.stop.assert_called()
    assert loki_queue_handler.queue.empty() is True


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


def test_worker_entrypoint_main_guards() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    worker_dir = repo_root / "app" / "workers"
    service_worker_dir = repo_root / "service_workers"

    def _close_coro(coro: object) -> None:
        closeable = coro
        if hasattr(closeable, "close"):
            closeable.close()  # type: ignore[attr-defined]
        return None

    with patch("asyncio.run", side_effect=_close_coro):
        runpy.run_path(str(worker_dir / "run_consent_sync.py"), run_name="__main__")
    with patch("asyncio.run", side_effect=_close_coro):
        runpy.run_path(
            str(service_worker_dir / "run_listmonk_duplicate_subscriber.py"), run_name="__main__"
        )
    with patch("asyncio.run", side_effect=_close_coro):
        runpy.run_path(str(worker_dir / "run_listmonk_reconcile.py"), run_name="__main__")
    with patch("asyncio.run", side_effect=_close_coro):
        runpy.run_path(str(worker_dir / "run_webhook_inbox_consumer.py"), run_name="__main__")
    with patch("asyncio.run", side_effect=_close_coro):
        runpy.run_path(str(worker_dir / "run_worker.py"), run_name="__main__")


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
        patch("app.workers.run_consent_sync.wait_for_database", new=AsyncMock()),
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
        patch("app.workers.run_listmonk_reconcile.wait_for_database", new=AsyncMock()),
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
        patch("app.workers.run_consent_sync.wait_for_database", new=AsyncMock()),
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
        patch("app.workers.run_listmonk_reconcile.wait_for_database", new=AsyncMock()),
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
        patch("app.workers.run_consent_sync.wait_for_database", new=AsyncMock()),
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
        patch("app.workers.run_listmonk_reconcile.wait_for_database", new=AsyncMock()),
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
        repo_instance = AsyncMock()
        repo_instance.enqueue.return_value = True
        repo_cls = MagicMock(return_value=repo_instance)
        session = AsyncMock()
        session_cm = AsyncMock()
        session_cm.__aenter__.return_value = session
        session_cm.__aexit__.return_value = False
        payload = {"type": "CREATE", "pass": {"user_id": 1}}
        with (
            patch("app.api.webhook.WebhookInboxRepository", repo_cls),
            patch("app.api.webhook.SessionLocal", return_value=session_cm),
        ):
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
            repo_instance.enqueue.assert_awaited_once()
    finally:
        if original_webhook is None:
            os.environ.pop("WEBHOOK", None)
        else:
            os.environ["WEBHOOK"] = original_webhook
        if original_token is None:
            os.environ.pop("WEBHOOK_AUTH_TOKEN", None)
        else:
            os.environ["WEBHOOK_AUTH_TOKEN"] = original_token
