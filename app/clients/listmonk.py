"""Listmonk client abstraction based on Python SDK."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, TypeVar

import httpx
import structlog

from app.config import Settings

logger = structlog.get_logger()
T = TypeVar("T")


class ListmonkClientError(Exception):
    """Raised when listmonk SDK is unavailable or returns unexpected payload."""


def _safe_info(event: str, **kwargs: object) -> None:
    """Best-effort logging that never affects business flow."""
    try:
        logger.info(event, **kwargs)
    except Exception:
        return


@dataclass(slots=True)
class SubscriberState:
    """Normalized subscriber state from Listmonk."""

    subscriber_id: int
    status: str
    list_ids: list[int]
    list_statuses: dict[int, str] | None = None

    def is_confirmed_for_any(self, target_list_ids: list[int]) -> bool:
        """True when subscriber is active/confirmed in target list."""
        if self.list_statuses and target_list_ids:
            return any(
                _is_confirmed_status(self.list_statuses.get(list_id, ""))
                for list_id in target_list_ids
            )
        if self.list_statuses:
            candidate_ids = target_list_ids or list(self.list_statuses.keys())
            for list_id in candidate_ids:
                status = self.list_statuses.get(list_id)
                if status and _is_confirmed_status(status):
                    return True
            return False
        normalized = self.status.strip().lower()
        if not _is_confirmed_status(normalized):
            return False
        if not target_list_ids:
            return True
        return any(list_id in target_list_ids for list_id in self.list_ids)

    def has_blocked_for_any(self, target_list_ids: list[int]) -> bool:
        """True when any target list status is blocked-like."""
        if not self.list_statuses:
            return False
        candidate_ids = target_list_ids or list(self.list_statuses.keys())
        for list_id in candidate_ids:
            status = self.list_statuses.get(list_id)
            if status and _is_blocked_status(status):
                return True
        return False

    def is_confirmed_for_all(self, target_list_ids: list[int]) -> bool:
        """True when all target list subscriptions are confirmed/active."""
        if not target_list_ids:
            return self.is_confirmed_for_any(target_list_ids=target_list_ids)
        if not self.list_statuses:
            # Legacy fallback when per-list statuses are not available.
            normalized = self.status.strip().lower()
            return _is_confirmed_status(normalized)
        for list_id in target_list_ids:
            status = self.list_statuses.get(list_id)
            if status is None or not _is_confirmed_status(status):
                return False
        return True


@dataclass(slots=True)
class SubscriberDelta:
    """Subscriber snapshot with updated_at for incremental sync."""

    subscriber_id: int
    status: str
    list_ids: list[int]
    updated_at: datetime
    email: str | None = None
    attributes: dict[str, Any] | None = None
    list_statuses: dict[int, str] | None = None


@dataclass(slots=True)
class SubscriberProfile:
    """Full subscriber snapshot used for repair/diagnostics."""

    subscriber_id: int
    email: str | None
    status: str
    list_ids: list[int]
    attributes: dict[str, Any] | None = None
    list_statuses: dict[int, str] | None = None


class ListmonkSDKClient:
    """Thin wrapper over listmonk Python SDK."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._logged_in = False

    async def _sdk_call(
        self,
        func: Callable[..., T],
        *args: object,
        action: str,
        retryable: bool = True,
        **kwargs: object,
    ) -> T:
        """Run blocking SDK call in thread with timeout and transient retries."""
        timeout_seconds = max(
            0.1, float(getattr(self._settings, "listmonk_request_timeout_seconds", 15.0))
        )
        max_retries = max(0, int(getattr(self._settings, "listmonk_request_max_retries", 2)))
        retry_backoff_seconds = max(
            0.0,
            float(getattr(self._settings, "listmonk_request_retry_backoff_seconds", 0.5)),
        )

        attempt = 0
        while True:
            try:
                return await asyncio.wait_for(
                    asyncio.to_thread(func, *args, **kwargs),
                    timeout=timeout_seconds,
                )
            except TimeoutError as exc:
                wrapped = ListmonkClientError(
                    f"Listmonk SDK call timeout after {timeout_seconds}s: action={action}"
                )
                if not retryable or attempt >= max_retries:
                    raise wrapped from exc
                logger.warning(
                    "listmonk_sdk_call_retry",
                    action=action,
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    error_type="TimeoutError",
                    timeout_seconds=timeout_seconds,
                )
            except (httpx.TimeoutException, httpx.NetworkError, httpx.TransportError) as exc:
                if not retryable or attempt >= max_retries:
                    raise
                logger.warning(
                    "listmonk_sdk_call_retry",
                    action=action,
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
            attempt += 1
            if retry_backoff_seconds > 0:
                await asyncio.sleep(retry_backoff_seconds * attempt)

    async def _ensure_login(self) -> None:
        if self._logged_in:
            return
        try:
            import listmonk  # type: ignore
        except ModuleNotFoundError as exc:
            raise ListmonkClientError("listmonk package is not installed") from exc

        await self._sdk_call(
            listmonk.set_url_base,
            self._settings.listmonk_url,
            action="set_url_base",
        )
        ok = await self._sdk_call(
            listmonk.login,
            self._settings.listmonk_user,
            self._settings.listmonk_password,
            action="login",
        )
        if not ok:
            raise ListmonkClientError("Listmonk SDK login failed")
        self._logged_in = True

    async def get_subscriber_state(self, *, subscriber_id: int) -> SubscriberState | None:
        """Fetch subscriber state from Listmonk SDK."""
        try:
            import listmonk  # type: ignore
        except ModuleNotFoundError as exc:
            raise ListmonkClientError("listmonk package is not installed") from exc

        await self._ensure_login()
        _safe_info(
            "listmonk_get_subscriber_state_request",
            subscriber_id=subscriber_id,
        )
        payload = await self._sdk_call(
            listmonk.subscriber_by_id,
            subscriber_id,
            action="subscriber_by_id",
        )
        if payload is None:
            _safe_info(
                "listmonk_get_subscriber_state_done",
                subscriber_id=subscriber_id,
                found=False,
            )
            return None

        status_raw = str(getattr(payload, "status", "") or "")
        normalized_status = _normalize_user_status(status_raw)
        lists_raw = getattr(payload, "lists", [])
        list_ids: list[int] = []
        if isinstance(lists_raw, (list, set, tuple)):
            for item in lists_raw:
                if isinstance(item, int):
                    list_ids.append(item)
                    continue
                if isinstance(item, dict) and "id" in item:
                    try:
                        list_ids.append(int(item["id"]))
                    except TypeError, ValueError:
                        continue
                    continue
                value = getattr(item, "id", None)
                if isinstance(value, int):
                    list_ids.append(value)

        if normalized_status is None:
            _safe_info(
                "listmonk_get_subscriber_state_done",
                subscriber_id=subscriber_id,
                found=False,
                reason="empty_status",
            )
            return None
        state = SubscriberState(
            subscriber_id=subscriber_id,
            status=normalized_status,
            list_ids=list_ids,
            list_statuses=_extract_list_statuses(payload),
        )
        _safe_info(
            "listmonk_get_subscriber_state_done",
            subscriber_id=subscriber_id,
            found=True,
            status=state.status,
            list_ids=state.list_ids,
        )
        return state

    async def get_subscriber_profile(self, *, subscriber_id: int) -> SubscriberProfile | None:
        """Fetch full subscriber snapshot from Listmonk SDK."""
        try:
            import listmonk  # type: ignore
        except ModuleNotFoundError as exc:
            raise ListmonkClientError("listmonk package is not installed") from exc

        await self._ensure_login()
        payload = await self._sdk_call(
            listmonk.subscriber_by_id,
            subscriber_id,
            action="subscriber_by_id",
        )
        if payload is None:
            return None
        state = await self.get_subscriber_state(subscriber_id=subscriber_id)
        if state is None:
            return None
        return SubscriberProfile(
            subscriber_id=state.subscriber_id,
            email=_extract_email(payload),
            status=state.status,
            list_ids=state.list_ids,
            attributes=_extract_attributes(payload),
            list_statuses=state.list_statuses,
        )

    async def get_subscriber_by_email(self, *, email: str) -> SubscriberState | None:
        """Fetch subscriber state from Listmonk by normalized email."""
        try:
            import listmonk  # type: ignore
        except ModuleNotFoundError as exc:
            raise ListmonkClientError("listmonk package is not installed") from exc

        await self._ensure_login()
        normalized_email = _normalize_email(email)
        if not normalized_email:
            raise ListmonkClientError("Subscriber email is required")
        _safe_info(
            "listmonk_get_subscriber_by_email_request",
            email=normalized_email,
        )
        payload = await self._sdk_call(
            listmonk.subscriber_by_email,
            normalized_email,
            action="subscriber_by_email",
        )
        subscriber_id = _extract_subscriber_id(payload)
        if payload is None or subscriber_id is None:
            _safe_info(
                "listmonk_get_subscriber_by_email_done",
                email=normalized_email,
                found=False,
            )
            return None
        state = await self.get_subscriber_state(subscriber_id=subscriber_id)
        if state is None:
            _safe_info(
                "listmonk_get_subscriber_by_email_done",
                email=normalized_email,
                found=False,
                subscriber_id=subscriber_id,
            )
            return None
        _safe_info(
            "listmonk_get_subscriber_by_email_done",
            email=normalized_email,
            found=True,
            subscriber_id=state.subscriber_id,
            status=state.status,
            list_ids=state.list_ids,
        )
        return state

    async def upsert_subscriber(
        self,
        *,
        email: str | None,
        list_ids: list[int],
        attributes: dict[str, object],
        subscriber_id: int | None = None,
    ) -> SubscriberState:
        """Create or update subscriber via SDK and return normalized state."""
        await self._ensure_login()
        normalized_email = _normalize_email(email)
        normalized_list_ids = _normalize_list_ids(list_ids)
        if not normalized_list_ids:
            raise ListmonkClientError("LISTMONK_LIST_IDS is empty or invalid")
        if subscriber_id is None and not normalized_email:
            raise ListmonkClientError("Subscriber email is required to create subscriber")
        resolved_name = _build_subscriber_name(
            attributes=attributes,
            fallback_email=normalized_email,
        )
        _safe_info(
            "listmonk_upsert_subscriber_request",
            subscriber_id=subscriber_id,
            email=normalized_email,
            name=resolved_name,
            list_ids=normalized_list_ids,
        )

        request_kwargs: dict[str, Any] = {
            "email": normalized_email,
            "name": resolved_name,
            "attribs": attributes,
            "subscriber_id": subscriber_id,
            "id": subscriber_id,
            "list_ids": normalized_list_ids,
        }
        if subscriber_id is not None:
            try:
                import listmonk  # type: ignore
            except ModuleNotFoundError as exc:
                raise ListmonkClientError("listmonk package is not installed") from exc
            subscriber = await self._sdk_call(
                listmonk.subscriber_by_id,
                subscriber_id,
                action="subscriber_by_id",
            )
            if subscriber is None:
                # Subscriber was removed from Listmonk, recreate by email.
                if not normalized_email:
                    raise ListmonkClientError(
                        f"subscriber_id={subscriber_id} not found and email is empty"
                    )
                return await self.upsert_subscriber(
                    email=normalized_email,
                    list_ids=normalized_list_ids,
                    attributes=attributes,
                    subscriber_id=None,
                )
            effective_subscriber_id = subscriber_id
            _apply_subscriber_update_fields(
                subscriber=subscriber,
                email=request_kwargs["email"],
                name=request_kwargs["name"],
                attribs=request_kwargs["attribs"],
            )
            current_status = getattr(subscriber, "status", None) or "enabled"
            try:
                response = await self._sdk_call(
                    listmonk.update_subscriber,
                    subscriber,
                    set(normalized_list_ids),
                    None,
                    current_status,
                    action="update_subscriber",
                    retryable=False,
                )
            except Exception as exc:
                if not _is_conflict_error(exc) or not normalized_email:
                    raise
                # subscriber_id may point to stale/mismatched record; fallback by unique email.
                existing_by_email = await self._sdk_call(
                    listmonk.subscriber_by_email,
                    normalized_email,
                    action="subscriber_by_email",
                )
                if existing_by_email is None:
                    raise ListmonkClientError(
                        f"Listmonk returned email conflict for subscriber_id={subscriber_id}, "
                        f"but subscriber_by_email returned None for email={normalized_email}"
                    ) from exc
                existing_id = _extract_subscriber_id(existing_by_email)
                if existing_id is not None:
                    effective_subscriber_id = existing_id
                _apply_subscriber_update_fields(
                    subscriber=existing_by_email,
                    email=request_kwargs["email"],
                    name=request_kwargs["name"],
                    attribs=request_kwargs["attribs"],
                )
                response = await self._sdk_call(
                    listmonk.update_subscriber,
                    existing_by_email,
                    set(normalized_list_ids),
                    None,
                    getattr(existing_by_email, "status", None) or "enabled",
                    action="update_subscriber",
                    retryable=False,
                )
            state = await self.get_subscriber_state(subscriber_id=effective_subscriber_id)
            if state is not None:
                _safe_info(
                    "listmonk_upsert_subscriber_done",
                    mode="update",
                    subscriber_id=state.subscriber_id,
                    status=state.status,
                    list_ids=state.list_ids,
                )
                return state
            status = (
                _extract_raw_status(response) or _extract_status(response) or str(current_status)
            )
            state = SubscriberState(
                subscriber_id=effective_subscriber_id,
                status=status,
                list_ids=normalized_list_ids,
            )
            _safe_info(
                "listmonk_upsert_subscriber_done",
                mode="update",
                subscriber_id=state.subscriber_id,
                status=state.status,
                list_ids=normalized_list_ids,
            )
            return state

        try:
            import listmonk  # type: ignore
        except ModuleNotFoundError as exc:
            raise ListmonkClientError("listmonk package is not installed") from exc
        try:
            response = await self._sdk_call(
                listmonk.create_subscriber,
                request_kwargs["email"],
                request_kwargs["name"],
                set(normalized_list_ids),
                False,
                request_kwargs["attribs"],
                action="create_subscriber",
                retryable=False,
            )
        except Exception as exc:
            if not _is_conflict_error(exc) or not request_kwargs["email"]:
                raise
            # Idempotent behavior: subscriber already exists, switch to update by email.
            existing = await self._sdk_call(
                listmonk.subscriber_by_email,
                request_kwargs["email"],
                action="subscriber_by_email",
            )
            if existing is None:
                raise ListmonkClientError(
                    f"Listmonk returned 409 for email={request_kwargs['email']}, "
                    "but subscriber_by_email returned None"
                ) from exc
            _apply_subscriber_update_fields(
                subscriber=existing,
                email=request_kwargs["email"],
                name=request_kwargs["name"],
                attribs=request_kwargs["attribs"],
            )
            response = await self._sdk_call(
                listmonk.update_subscriber,
                existing,
                set(normalized_list_ids),
                None,
                getattr(existing, "status", None) or "enabled",
                action="update_subscriber",
                retryable=False,
            )
        created_id = _extract_subscriber_id(response)
        if created_id is None:
            raise ListmonkClientError("Listmonk SDK did not return subscriber id for create")
        state = await self.get_subscriber_state(subscriber_id=created_id)
        if state is not None:
            _safe_info(
                "listmonk_upsert_subscriber_done",
                mode="create",
                subscriber_id=state.subscriber_id,
                status=state.status,
                list_ids=state.list_ids,
            )
            return state
        status = _extract_status(response) or "enabled"
        state = SubscriberState(
            subscriber_id=created_id, status=status, list_ids=normalized_list_ids
        )
        _safe_info(
            "listmonk_upsert_subscriber_done",
            mode="create",
            subscriber_id=state.subscriber_id,
            status=state.status,
            list_ids=state.list_ids,
        )
        return state

    async def restore_subscriber(
        self,
        *,
        email: str | None,
        list_ids: list[int],
        attributes: dict[str, object] | None,
        desired_status: str | None,
    ) -> SubscriberState:
        """Create subscriber if missing and try to apply desired status."""
        _safe_info(
            "listmonk_restore_subscriber_request",
            email=email,
            list_ids=list_ids,
            desired_status=desired_status,
        )
        if not email:
            raise ListmonkClientError("Cannot restore subscriber without email")

        state = await self.upsert_subscriber(
            email=email,
            list_ids=list_ids,
            attributes=attributes or {},
            subscriber_id=None,
        )
        target_status = _normalize_status_for_restore(desired_status)
        if target_status is None:
            _safe_info(
                "listmonk_restore_subscriber_done",
                subscriber_id=state.subscriber_id,
                status=state.status,
                list_ids=state.list_ids,
                status_changed=False,
            )
            return state

        try:
            import listmonk  # type: ignore
        except ModuleNotFoundError as exc:
            raise ListmonkClientError("listmonk package is not installed") from exc
        subscriber = await self._sdk_call(
            listmonk.subscriber_by_id,
            state.subscriber_id,
            action="subscriber_by_id",
        )
        if subscriber is None:
            return state
        await self._sdk_call(
            listmonk.update_subscriber,
            subscriber,
            set(list_ids),
            None,
            target_status,
            action="update_subscriber",
            retryable=False,
        )
        refreshed = await self.get_subscriber_state(subscriber_id=state.subscriber_id)
        if refreshed is not None:
            _safe_info(
                "listmonk_restore_subscriber_done",
                subscriber_id=refreshed.subscriber_id,
                status=refreshed.status,
                list_ids=refreshed.list_ids,
                status_changed=True,
            )
            return refreshed
        restored = SubscriberState(
            subscriber_id=state.subscriber_id,
            status=target_status,
            list_ids=list_ids,
        )
        _safe_info(
            "listmonk_restore_subscriber_done",
            subscriber_id=restored.subscriber_id,
            status=restored.status,
            list_ids=restored.list_ids,
            status_changed=True,
        )
        return restored

    async def delete_subscriber(self, *, subscriber_id: int) -> None:
        """Delete subscriber via SDK by id."""
        _safe_info(
            "listmonk_delete_subscriber_request",
            subscriber_id=subscriber_id,
        )
        await self._ensure_login()
        try:
            import listmonk  # type: ignore
        except ModuleNotFoundError as exc:
            raise ListmonkClientError("listmonk package is not installed") from exc
        await self._sdk_call(
            listmonk.delete_subscriber,
            None,
            subscriber_id,
            action="delete_subscriber",
            retryable=False,
        )
        _safe_info(
            "listmonk_delete_subscriber_done",
            subscriber_id=subscriber_id,
        )

    async def get_updated_subscribers(
        self,
        *,
        list_id: int,
        watermark_updated_at: datetime | None,
        watermark_subscriber_id: int | None,
        limit: int,
    ) -> list[SubscriberDelta]:
        """Fetch incremental subscriber changes from Listmonk for one list."""
        _safe_info(
            "listmonk_get_updated_subscribers_request",
            list_id=list_id,
            watermark_updated_at=watermark_updated_at.isoformat() if watermark_updated_at else None,
            watermark_subscriber_id=watermark_subscriber_id,
            limit=limit,
        )
        try:
            import listmonk  # type: ignore
        except ModuleNotFoundError as exc:
            raise ListmonkClientError("listmonk package is not installed") from exc

        await self._ensure_login()
        subscribers = await self._sdk_call(
            listmonk.subscribers,
            None,
            list_id,
            action="subscribers",
        )
        deltas: list[SubscriberDelta] = []
        for item in subscribers:
            subscriber_id = _extract_subscriber_id(item)
            status = _extract_status(item)
            if subscriber_id is None or not status:
                continue
            updated_at = _extract_updated_at(item)
            if not _is_after_watermark(
                updated_at=updated_at,
                subscriber_id=subscriber_id,
                watermark_updated_at=watermark_updated_at,
                watermark_subscriber_id=watermark_subscriber_id,
            ):
                continue
            deltas.append(
                SubscriberDelta(
                    subscriber_id=subscriber_id,
                    status=status,
                    list_ids=_extract_list_ids(item),
                    updated_at=updated_at,
                    email=_extract_email(item),
                    attributes=_extract_attributes(item),
                    list_statuses=_extract_list_statuses(item),
                )
            )

        deltas.sort(key=lambda item: (item.updated_at, item.subscriber_id))
        result = deltas[: max(1, limit)]
        _safe_info(
            "listmonk_get_updated_subscribers_done",
            list_id=list_id,
            count=len(result),
            first_subscriber_id=result[0].subscriber_id if result else None,
            last_subscriber_id=result[-1].subscriber_id if result else None,
        )
        return result


def _extract_subscriber_id(payload: object) -> int | None:
    if payload is None:
        return None
    if isinstance(payload, dict):
        for key in ("id", "subscriber_id", "subscriberID"):
            value = payload.get(key)
            if isinstance(value, int):
                return value
            if isinstance(value, str) and value.isdigit():
                return int(value)
    value = getattr(payload, "id", None)
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    value = getattr(payload, "subscriber_id", None)
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _extract_status(payload: object) -> str | None:
    if payload is None:
        return None
    if isinstance(payload, dict):
        value = payload.get("status")
        if isinstance(value, str):
            return _normalize_user_status(value)
    value = getattr(payload, "status", None)
    if isinstance(value, str):
        return _normalize_user_status(value)
    return None


def _extract_raw_status(payload: object) -> str | None:
    if payload is None:
        return None
    if isinstance(payload, dict):
        value = payload.get("status")
    else:
        value = getattr(payload, "status", None)
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    return normalized or None


def _extract_updated_at(payload: object) -> datetime:
    value = getattr(payload, "updated_at", None)
    if isinstance(value, datetime):
        return _to_utc(value)
    created = getattr(payload, "created_at", None)
    if isinstance(created, datetime):
        return _to_utc(created)
    return datetime.min.replace(tzinfo=UTC)


def _extract_list_ids(payload: object) -> list[int]:
    lists_raw = getattr(payload, "lists", [])
    list_ids: list[int] = []
    if isinstance(lists_raw, (list, set, tuple)):
        for item in lists_raw:
            if isinstance(item, int):
                list_ids.append(item)
                continue
            if isinstance(item, dict) and "id" in item:
                try:
                    list_ids.append(int(item["id"]))
                except TypeError, ValueError:
                    continue
                continue
            value = getattr(item, "id", None)
            if isinstance(value, int):
                list_ids.append(value)
    return list_ids


def _extract_list_statuses(payload: object) -> dict[int, str]:
    lists_raw = getattr(payload, "lists", [])
    statuses: dict[int, str] = {}
    if isinstance(lists_raw, (list, set, tuple)):
        for item in lists_raw:
            list_id: int | None = None
            status: str | None = None
            if isinstance(item, dict):
                raw_id = item.get("id")
                if isinstance(raw_id, int):
                    list_id = raw_id
                elif isinstance(raw_id, str) and raw_id.isdigit():
                    list_id = int(raw_id)
                # Critical: this is subscriber status inside the list (not list lifecycle status).
                raw_subscription_status = item.get("subscription_status")
                if isinstance(raw_subscription_status, str) and raw_subscription_status.strip():
                    status = raw_subscription_status.strip()
                else:
                    raw_status = item.get("status")
                    if isinstance(raw_status, str) and raw_status.strip():
                        status = raw_status.strip()
            else:
                raw_id = getattr(item, "id", None)
                if isinstance(raw_id, int):
                    list_id = raw_id
                elif isinstance(raw_id, str) and raw_id.isdigit():
                    list_id = int(raw_id)
                raw_subscription_status = getattr(item, "subscription_status", None)
                if isinstance(raw_subscription_status, str) and raw_subscription_status.strip():
                    status = raw_subscription_status.strip()
                else:
                    raw_status = getattr(item, "status", None)
                    if isinstance(raw_status, str) and raw_status.strip():
                        status = raw_status.strip()
            if list_id is not None and status is not None:
                statuses[list_id] = status
    return statuses


def _extract_email(payload: object) -> str | None:
    if isinstance(payload, dict):
        value = payload.get("email")
        if isinstance(value, str):
            return value
    value = getattr(payload, "email", None)
    if isinstance(value, str):
        return value
    return None


def _extract_attributes(payload: object) -> dict[str, Any] | None:
    if isinstance(payload, dict):
        value = payload.get("attribs") or payload.get("attributes")
        if isinstance(value, dict):
            return dict(value)
    for field in ("attribs", "attributes"):
        value = getattr(payload, field, None)
        if isinstance(value, dict):
            return dict(value)
    return None


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _is_after_watermark(
    *,
    updated_at: datetime,
    subscriber_id: int,
    watermark_updated_at: datetime | None,
    watermark_subscriber_id: int | None,
) -> bool:
    if watermark_updated_at is None:
        return True
    watermark_dt = _to_utc(watermark_updated_at)
    if updated_at > watermark_dt:
        return True
    if updated_at < watermark_dt:
        return False
    return subscriber_id > (watermark_subscriber_id or 0)


def _is_conflict_error(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if response is not None and getattr(response, "status_code", None) == httpx.codes.CONFLICT:
        return True
    text = str(exc).lower()
    if "409" in text and "conflict" in text:
        return True
    if "duplicate key value violates unique constraint" in text:
        return True
    if "subscribers_email_key" in text:
        return True
    return False


def _normalize_status_for_restore(status: str | None) -> str | None:
    return _normalize_user_status(status)


def _normalize_user_status(status: str | None) -> str | None:
    if status is None:
        return None
    normalized = status.strip().lower()
    if not normalized:
        return None
    if _is_blocked_status(normalized):
        return "blocklisted"
    return "enabled"


def _apply_subscriber_update_fields(
    *,
    subscriber: object,
    email: str | None,
    name: str,
    attribs: dict[str, object],
) -> None:
    """Best-effort mutate SDK subscriber object/dict before update call."""
    if isinstance(subscriber, dict):
        subscriber["email"] = email
        subscriber["name"] = name
        subscriber["attribs"] = attribs
        return
    try:
        setattr(subscriber, "email", email)
        setattr(subscriber, "name", name)
        setattr(subscriber, "attribs", attribs)
    except Exception:
        # Some SDK wrappers may use immutable objects; update_subscriber still receives attribs arg.
        return


def _is_blocked_status(status: str) -> bool:
    return status.strip().lower() in {"blocked", "blocklisted", "blacklisted"}


def _is_confirmed_status(status: str) -> bool:
    return status.strip().lower() in {"enabled", "confirmed", "active"}


def _normalize_email(email: str | None) -> str | None:
    if email is None:
        return None
    normalized = email.strip().lower()
    return normalized or None


def _normalize_list_ids(list_ids: list[int]) -> list[int]:
    unique: set[int] = set()
    for list_id in list_ids:
        if isinstance(list_id, int) and list_id > 0:
            unique.add(list_id)
    return sorted(unique)


def _build_subscriber_name(*, attributes: dict[str, object], fallback_email: str | None) -> str:
    fio = attributes.get("fio")
    if isinstance(fio, str):
        normalized_fio = fio.strip()
        if normalized_fio:
            return normalized_fio

    # Common Teyca payload path: separate first/last/patronymic names.
    parts: list[str] = []
    for key in ("last_name", "first_name", "pat_name"):
        raw = attributes.get(key)
        if isinstance(raw, str):
            normalized = raw.strip()
            if normalized:
                parts.append(normalized)
    if parts:
        return " ".join(parts)

    return fallback_email or ""
