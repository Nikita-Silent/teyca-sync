from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from app.clients.listmonk import (
    ListmonkClientError,
    ListmonkSDKClient,
    SubscriberState,
    _extract_attributes,
    _extract_email,
    _extract_list_ids,
    _extract_status,
    _extract_subscriber_id,
    _extract_updated_at,
    _is_after_watermark,
    _is_blocked_status,
    _is_conflict_error,
    _is_confirmed_status,
    _normalize_status_for_restore,
    _to_utc,
)


async def _run_to_thread(func: object, *args: object, **kwargs: object) -> object:
    return func(*args, **kwargs)


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        listmonk_url="http://listmonk",
        listmonk_user="u",
        listmonk_password="p",
    )


def test_subscriber_state_helpers() -> None:
    state = SubscriberState(
        subscriber_id=1,
        status="enabled",
        list_ids=[1, 2],
        list_statuses={1: "confirmed", 2: "unconfirmed"},
    )
    assert state.is_confirmed_for_any([1]) is True
    assert state.is_confirmed_for_all([1]) is True
    assert state.is_confirmed_for_all([1, 2]) is False
    assert state.has_blocked_for_any([1, 2]) is False

    state = SubscriberState(
        subscriber_id=2,
        status="blocked",
        list_ids=[1],
        list_statuses={1: "blocklisted"},
    )
    assert state.has_blocked_for_any([1]) is True

    legacy = SubscriberState(subscriber_id=3, status="confirmed", list_ids=[1], list_statuses=None)
    assert legacy.is_confirmed_for_any([1]) is True
    assert legacy.is_confirmed_for_any([]) is True
    assert SubscriberState(subscriber_id=4, status="disabled", list_ids=[], list_statuses=None).is_confirmed_for_all(
        [1]
    ) is False
    assert SubscriberState(
        subscriber_id=5,
        status="enabled",
        list_ids=[],
        list_statuses={1: "unconfirmed"},
    ).is_confirmed_for_any([]) is False
    assert SubscriberState(
        subscriber_id=6,
        status="enabled",
        list_ids=[],
        list_statuses={1: "confirmed"},
    ).is_confirmed_for_any([]) is True
    assert SubscriberState(subscriber_id=7, status="disabled", list_ids=[], list_statuses=None).is_confirmed_for_any(
        [1]
    ) is False
    assert SubscriberState(subscriber_id=8, status="enabled", list_ids=[2], list_statuses=None).is_confirmed_for_all(
        []
    ) is True


def test_listmonk_extract_helpers() -> None:
    assert _extract_subscriber_id({"id": 1}) == 1
    assert _extract_subscriber_id({"subscriber_id": "2"}) == 2
    assert _extract_subscriber_id(SimpleNamespace(id="3")) == 3
    assert _extract_subscriber_id(SimpleNamespace(subscriber_id=4)) == 4
    assert _extract_subscriber_id(SimpleNamespace(subscriber_id="5")) == 5
    assert _extract_subscriber_id(None) is None

    assert _extract_status({"status": "enabled"}) == "enabled"

    class _DictObj(dict):
        status = "enabled"

    assert _extract_status(_DictObj(status=1)) == "enabled"
    assert _extract_status(SimpleNamespace(status="confirmed")) == "confirmed"
    assert _extract_status(SimpleNamespace(status=10)) is None
    assert _extract_status(None) is None

    now = datetime.now(UTC)
    assert _extract_updated_at(SimpleNamespace(updated_at=now)) == now
    assert _extract_updated_at(SimpleNamespace(created_at=now)) == now
    assert _extract_updated_at(SimpleNamespace()) == datetime.min.replace(tzinfo=UTC)

    assert _extract_list_ids(SimpleNamespace(lists=[1, {"id": "2"}, SimpleNamespace(id=3)])) == [1, 2, 3]
    assert _extract_list_ids(SimpleNamespace(lists=[{"id": "bad"}, SimpleNamespace(id="7")])) == []
    assert _extract_list_ids(SimpleNamespace(lists=None)) == []

    assert _extract_email({"email": "x@y.z"}) == "x@y.z"
    assert _extract_email(SimpleNamespace(email="a@b.c")) == "a@b.c"
    assert _extract_email({"email": 1}) is None
    assert _extract_email(SimpleNamespace()) is None

    assert _extract_attributes({"attribs": {"a": 1}}) == {"a": 1}
    assert _extract_attributes({"attributes": {"a": 1}}) == {"a": 1}
    assert _extract_attributes({"attribs": "x", "attributes": "y"}) is None
    assert _extract_attributes(SimpleNamespace(attribs={"a": 1})) == {"a": 1}
    assert _extract_attributes(SimpleNamespace(attributes={"a": 1})) == {"a": 1}
    assert _extract_attributes(None) is None

    naive = datetime(2026, 3, 6, 12, 0)
    aware = datetime(2026, 3, 6, 12, 0, tzinfo=UTC)
    assert _to_utc(naive).tzinfo is UTC
    assert _to_utc(aware) == aware

    assert _is_after_watermark(
        updated_at=aware,
        subscriber_id=2,
        watermark_updated_at=None,
        watermark_subscriber_id=None,
    )
    assert _is_after_watermark(
        updated_at=aware,
        subscriber_id=3,
        watermark_updated_at=aware,
        watermark_subscriber_id=2,
    )
    assert not _is_after_watermark(
        updated_at=aware,
        subscriber_id=1,
        watermark_updated_at=aware,
        watermark_subscriber_id=2,
    )
    assert not _is_after_watermark(
        updated_at=datetime(2026, 3, 6, 4, 0, tzinfo=UTC),
        subscriber_id=10,
        watermark_updated_at=aware,
        watermark_subscriber_id=2,
    )

    err = RuntimeError("409 conflict")
    assert _is_conflict_error(err)
    resp_err = RuntimeError("boom")
    setattr(resp_err, "response", SimpleNamespace(status_code=httpx.codes.CONFLICT))
    assert _is_conflict_error(resp_err)
    assert not _is_conflict_error(RuntimeError("other"))

    assert _normalize_status_for_restore("blocked") == "blocklisted"
    assert _normalize_status_for_restore("confirmed") == "enabled"
    assert _normalize_status_for_restore("other") == "disabled"
    assert _normalize_status_for_restore(" ") is None
    assert _normalize_status_for_restore(None) is None

    assert _is_blocked_status("blocklisted")
    assert _is_confirmed_status("enabled")


@pytest.mark.asyncio
async def test_ensure_login_and_get_subscriber_state() -> None:
    fake = SimpleNamespace()
    fake.set_url_base = MagicMock()
    fake.login = MagicMock(return_value=True)
    fake.subscriber_by_id = MagicMock(
        return_value=SimpleNamespace(
            status="enabled",
            lists=[{"id": 1, "subscription_status": "confirmed"}],
        )
    )

    with patch.dict("sys.modules", {"listmonk": fake}), patch(
        "app.clients.listmonk.asyncio.to_thread", new=AsyncMock(side_effect=_run_to_thread)
    ):
        client = ListmonkSDKClient(_settings())
        await client._ensure_login()
        await client._ensure_login()
        state = await client.get_subscriber_state(subscriber_id=11)

    fake.login.assert_called_once()
    assert state is not None
    assert state.subscriber_id == 11
    assert state.list_statuses == {1: "confirmed"}


@pytest.mark.asyncio
async def test_get_subscriber_state_returns_none_for_empty_payload_or_status() -> None:
    fake = SimpleNamespace()
    fake.set_url_base = MagicMock()
    fake.login = MagicMock(return_value=True)
    fake.subscriber_by_id = MagicMock(side_effect=[None, SimpleNamespace(status="", lists=[{"id": "bad"}])])

    with patch.dict("sys.modules", {"listmonk": fake}), patch(
        "app.clients.listmonk.asyncio.to_thread", new=AsyncMock(side_effect=_run_to_thread)
    ):
        client = ListmonkSDKClient(_settings())
        assert await client.get_subscriber_state(subscriber_id=1) is None
        assert await client.get_subscriber_state(subscriber_id=2) is None


@pytest.mark.asyncio
async def test_get_subscriber_state_collects_object_list_ids() -> None:
    fake = SimpleNamespace()
    fake.set_url_base = MagicMock()
    fake.login = MagicMock(return_value=True)
    fake.subscriber_by_id = MagicMock(
        return_value=SimpleNamespace(
            status="enabled",
            lists=[SimpleNamespace(id=9, status="confirmed")],
        )
    )

    with patch.dict("sys.modules", {"listmonk": fake}), patch(
        "app.clients.listmonk.asyncio.to_thread", new=AsyncMock(side_effect=_run_to_thread)
    ):
        client = ListmonkSDKClient(_settings())
        state = await client.get_subscriber_state(subscriber_id=3)
    assert state is not None
    assert state.list_ids == [9]


@pytest.mark.asyncio
async def test_ensure_login_failure() -> None:
    fake = SimpleNamespace()
    fake.set_url_base = MagicMock()
    fake.login = MagicMock(return_value=False)

    with patch.dict("sys.modules", {"listmonk": fake}), patch(
        "app.clients.listmonk.asyncio.to_thread", new=AsyncMock(side_effect=_run_to_thread)
    ):
        client = ListmonkSDKClient(_settings())
        with pytest.raises(ListmonkClientError):
            await client._ensure_login()


@pytest.mark.asyncio
async def test_upsert_subscriber_create_update_and_conflict_paths() -> None:
    fake = SimpleNamespace()
    fake.set_url_base = MagicMock()
    fake.login = MagicMock(return_value=True)
    fake.subscriber_by_id = MagicMock(return_value=SimpleNamespace(status="enabled", id=77))
    fake.create_subscriber = MagicMock(return_value={"id": 77, "status": "enabled"})
    fake.update_subscriber = MagicMock(return_value={"status": "enabled"})
    fake.subscriber_by_email = MagicMock(return_value=SimpleNamespace(id=88, status="enabled"))

    with patch.dict("sys.modules", {"listmonk": fake}), patch(
        "app.clients.listmonk.asyncio.to_thread", new=AsyncMock(side_effect=_run_to_thread)
    ):
        client = ListmonkSDKClient(_settings())
        with pytest.raises(ListmonkClientError):
            await client.upsert_subscriber(email=None, list_ids=[1], attributes={}, subscriber_id=None)

        state = await client.upsert_subscriber(
            email="x@y.z",
            list_ids=[1],
            attributes={"fio": "X"},
            subscriber_id=None,
        )
        assert state.subscriber_id == 77

        state = await client.upsert_subscriber(
            email="x@y.z",
            list_ids=[1],
            attributes={"fio": "X"},
            subscriber_id=77,
        )
        assert state.subscriber_id == 77

        conflict = RuntimeError("409 conflict")
        fake.create_subscriber.side_effect = conflict
        fake.update_subscriber.return_value = {"id": 88, "status": "enabled"}
        fake.subscriber_by_email.return_value = SimpleNamespace(id=88, status="enabled", email="x@y.z")
        state = await client.upsert_subscriber(
            email="x@y.z",
            list_ids=[1],
            attributes={"fio": "X"},
            subscriber_id=None,
        )
        assert state.subscriber_id in {77, 88}

        fake.subscriber_by_email.return_value = None
        with pytest.raises(ListmonkClientError):
            await client.upsert_subscriber(
                email="x@y.z",
                list_ids=[1],
                attributes={"fio": "X"},
                subscriber_id=None,
            )


@pytest.mark.asyncio
async def test_upsert_subscriber_edge_paths() -> None:
    fake = SimpleNamespace()
    fake.set_url_base = MagicMock()
    fake.login = MagicMock(return_value=True)
    fake.subscriber_by_id = MagicMock(return_value=None)
    fake.create_subscriber = MagicMock(return_value={"status": "enabled"})
    fake.update_subscriber = MagicMock(return_value={"status": "enabled"})
    fake.subscriber_by_email = MagicMock(return_value=None)

    with patch.dict("sys.modules", {"listmonk": fake}), patch(
        "app.clients.listmonk.asyncio.to_thread", new=AsyncMock(side_effect=_run_to_thread)
    ):
        client = ListmonkSDKClient(_settings())
        with pytest.raises(ListmonkClientError):
            await client.upsert_subscriber(
                email=None,
                list_ids=[1],
                attributes={},
                subscriber_id=10,
            )

        fake.subscriber_by_id.return_value = None
        fake.create_subscriber.return_value = {"id": 10, "status": "enabled"}
        with patch.object(client, "upsert_subscriber", wraps=client.upsert_subscriber) as wrapped:
            await client.upsert_subscriber(
                email="x@y.z",
                list_ids=[1],
                attributes={},
                subscriber_id=10,
            )
            assert wrapped.call_count >= 2

        fake.create_subscriber.return_value = {"status": "enabled"}
        with patch.object(client, "get_subscriber_state", new=AsyncMock(return_value=None)):
            with pytest.raises(ListmonkClientError):
                await client.upsert_subscriber(
                    email="x@y.z",
                    list_ids=[1],
                    attributes={},
                    subscriber_id=None,
                )


@pytest.mark.asyncio
async def test_restore_delete_and_get_updated_subscribers() -> None:
    fake = SimpleNamespace()
    fake.set_url_base = MagicMock()
    fake.login = MagicMock(return_value=True)
    fake.subscriber_by_id = MagicMock(return_value=SimpleNamespace(id=1, status="enabled", lists=[1]))
    fake.update_subscriber = MagicMock(return_value={"status": "enabled"})
    fake.delete_subscriber = MagicMock()
    fake.create_subscriber = MagicMock(return_value={"id": 1, "status": "enabled"})
    fake.subscribers = MagicMock(
        return_value=[
            SimpleNamespace(
                id=1,
                status="enabled",
                lists=[{"id": 1, "subscription_status": "confirmed"}],
                email="x@y.z",
                attribs={"user_id": 1},
                updated_at=datetime(2026, 3, 6, 6, 0, tzinfo=UTC),
            ),
            SimpleNamespace(
                id=2,
                status="",
                lists=[1],
                updated_at=datetime(2026, 3, 6, 6, 0, tzinfo=UTC),
            ),
        ]
    )

    with patch.dict("sys.modules", {"listmonk": fake}), patch(
        "app.clients.listmonk.asyncio.to_thread", new=AsyncMock(side_effect=_run_to_thread)
    ):
        client = ListmonkSDKClient(_settings())

        with pytest.raises(ListmonkClientError):
            await client.restore_subscriber(
                email=None,
                list_ids=[1],
                attributes=None,
                desired_status="confirmed",
            )

        restored = await client.restore_subscriber(
            email="x@y.z",
            list_ids=[1],
            attributes={"user_id": 1},
            desired_status="confirmed",
        )
        assert restored.subscriber_id == 1

        passthrough = await client.restore_subscriber(
            email="x@y.z",
            list_ids=[1],
            attributes={"user_id": 1},
            desired_status=None,
        )
        assert passthrough.subscriber_id == 1

        await client.delete_subscriber(subscriber_id=1)
        fake.delete_subscriber.assert_called_once()

        deltas = await client.get_updated_subscribers(
            list_id=1,
            watermark_updated_at=datetime(2026, 3, 6, 5, 0, tzinfo=UTC),
            watermark_subscriber_id=0,
            limit=1,
        )
        assert len(deltas) == 1
        assert deltas[0].subscriber_id == 1


@pytest.mark.asyncio
async def test_restore_and_import_error_paths() -> None:
    fake = SimpleNamespace()
    fake.set_url_base = MagicMock()
    fake.login = MagicMock(return_value=True)
    fake.subscriber_by_id = MagicMock(return_value=None)
    fake.update_subscriber = MagicMock(return_value={"status": "enabled"})
    fake.delete_subscriber = MagicMock()
    fake.create_subscriber = MagicMock(return_value={"id": 1, "status": "enabled"})
    fake.subscribers = MagicMock(return_value=[])

    with patch.dict("sys.modules", {"listmonk": fake}), patch(
        "app.clients.listmonk.asyncio.to_thread", new=AsyncMock(side_effect=_run_to_thread)
    ):
        client = ListmonkSDKClient(_settings())
        with patch.object(client, "get_subscriber_state", new=AsyncMock(return_value=None)):
            restored = await client.restore_subscriber(
                email="x@y.z",
                list_ids=[1],
                attributes={"user_id": 1},
                desired_status="confirmed",
            )
            assert restored.status == "enabled"

    with patch("builtins.__import__", side_effect=ModuleNotFoundError("listmonk")):
        client = ListmonkSDKClient(_settings())
        with pytest.raises(ListmonkClientError):
            await client._ensure_login()
        with pytest.raises(ListmonkClientError):
            await client.get_subscriber_state(subscriber_id=1)
        with pytest.raises(ListmonkClientError):
            await client.delete_subscriber(subscriber_id=1)
        with pytest.raises(ListmonkClientError):
            await client.get_updated_subscribers(
                list_id=1,
                watermark_updated_at=None,
                watermark_subscriber_id=None,
                limit=1,
            )


@pytest.mark.asyncio
async def test_import_error_branches_after_login_and_other_edges() -> None:
    client = ListmonkSDKClient(_settings())
    client._logged_in = True
    with patch("builtins.__import__", side_effect=ModuleNotFoundError("listmonk")):
        with pytest.raises(ListmonkClientError):
            await client.upsert_subscriber(email="x@y.z", list_ids=[1], attributes={}, subscriber_id=10)
        with pytest.raises(ListmonkClientError):
            await client.upsert_subscriber(email="x@y.z", list_ids=[1], attributes={}, subscriber_id=None)
        with pytest.raises(ListmonkClientError):
            await client.delete_subscriber(subscriber_id=1)

        with patch.object(
            client,
            "upsert_subscriber",
            new=AsyncMock(return_value=SubscriberState(subscriber_id=1, status="enabled", list_ids=[1])),
        ):
            with pytest.raises(ListmonkClientError):
                await client.restore_subscriber(
                    email="x@y.z",
                    list_ids=[1],
                    attributes={},
                    desired_status="confirmed",
                )


@pytest.mark.asyncio
async def test_upsert_update_fallback_status_when_state_missing() -> None:
    fake = SimpleNamespace()
    fake.set_url_base = MagicMock()
    fake.login = MagicMock(return_value=True)
    fake.subscriber_by_id = MagicMock(return_value=SimpleNamespace(status="confirmed", id=10))
    fake.update_subscriber = MagicMock(return_value={"status": "updated"})
    with patch.dict("sys.modules", {"listmonk": fake}), patch(
        "app.clients.listmonk.asyncio.to_thread", new=AsyncMock(side_effect=_run_to_thread)
    ):
        client = ListmonkSDKClient(_settings())
        with patch.object(client, "get_subscriber_state", new=AsyncMock(return_value=None)):
            state = await client.upsert_subscriber(
                email="x@y.z",
                list_ids=[1],
                attributes={},
                subscriber_id=10,
            )
    assert state.status == "updated"


@pytest.mark.asyncio
async def test_upsert_non_conflict_error_and_restore_fallback_and_watermark_skip() -> None:
    fake = SimpleNamespace()
    fake.set_url_base = MagicMock()
    fake.login = MagicMock(return_value=True)
    fake.create_subscriber = MagicMock(side_effect=RuntimeError("boom"))
    fake.subscriber_by_id = MagicMock(return_value=SimpleNamespace(id=1, status="enabled", lists=[1]))
    fake.update_subscriber = MagicMock(return_value={"status": "enabled"})
    fake.subscribers = MagicMock(
        return_value=[
            SimpleNamespace(
                id=1,
                status="enabled",
                lists=[{"id": 1, "status": "confirmed"}],
                updated_at=datetime(2026, 3, 6, 1, 0, tzinfo=UTC),
            )
        ]
    )
    with patch.dict("sys.modules", {"listmonk": fake}), patch(
        "app.clients.listmonk.asyncio.to_thread", new=AsyncMock(side_effect=_run_to_thread)
    ):
        client = ListmonkSDKClient(_settings())
        with pytest.raises(RuntimeError):
            await client.upsert_subscriber(email="x@y.z", list_ids=[1], attributes={}, subscriber_id=None)

        with patch.object(
            client,
            "upsert_subscriber",
            new=AsyncMock(return_value=SubscriberState(subscriber_id=1, status="enabled", list_ids=[1])),
        ), patch.object(client, "get_subscriber_state", new=AsyncMock(return_value=None)):
            restored = await client.restore_subscriber(
                email="x@y.z",
                list_ids=[1],
                attributes={},
                desired_status="confirmed",
            )
        assert restored.status == "enabled"

        deltas = await client.get_updated_subscribers(
            list_id=1,
            watermark_updated_at=datetime(2026, 3, 6, 2, 0, tzinfo=UTC),
            watermark_subscriber_id=100,
            limit=10,
        )
        assert deltas == []
