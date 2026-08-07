from __future__ import annotations

from app.config import Settings
from app.mq.queues import QUEUE_CREATE_DEAD
from app.workers.run_queue_consumers import build_main_queue_args


def test_build_main_queue_args_disabled_when_zero() -> None:
    assert build_main_queue_args(0, QUEUE_CREATE_DEAD) is None


def test_build_main_queue_args_disabled_when_negative() -> None:
    assert build_main_queue_args(-1, QUEUE_CREATE_DEAD) is None


def test_build_main_queue_args_enables_dlx_with_delivery_limit() -> None:
    args = build_main_queue_args(10, QUEUE_CREATE_DEAD)

    assert args == {
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": QUEUE_CREATE_DEAD,
        "x-max-delivery-count": 10,
    }


def test_default_settings_enable_max_delivery_count() -> None:
    assert Settings().rabbitmq_main_queue_max_delivery_count > 0
