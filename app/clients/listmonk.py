"""Listmonk client abstraction based on Python SDK."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog

from app.config import Settings

logger = structlog.get_logger()


class ListmonkClientError(Exception):
    """Raised when listmonk SDK is unavailable or returns unexpected payload."""


def _safe_info(event: str, **kwargs: object) -> None:
    """
    Log an informational event without allowing logging errors to propagate.
    
    Parameters:
        event (str): Message or message template to log.
        **kwargs (object): Additional context forwarded to the logger; any exceptions raised during logging are caught and ignored.
    """
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
        """
        Determine whether the subscriber is confirmed (active/enabled) for any of the specified lists.
        
        If per-list statuses are available, those are used. If no per-list statuses exist, the subscriber's overall status is used and membership in the specified lists is checked. An empty `target_list_ids` means "any list" (i.e., check all lists the subscriber belongs to).
        
        Parameters:
            target_list_ids (list[int]): List IDs to check; may be empty to indicate all lists.
        
        Returns:
            bool: `True` if the subscriber is confirmed for any of the specified lists, `False` otherwise.
        """
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
        """
        Determine whether the subscriber has a blocked-like status for any of the given lists.
        
        If `target_list_ids` is empty, the method checks all lists present in `self.list_statuses`. A blocked-like status includes values such as "blocked", "blocklisted", or "blacklisted".
        
        Parameters:
            target_list_ids (list[int]): List IDs to check; an empty list means "check all known lists".
        
        Returns:
            bool: `True` if any of the target lists has a blocked-like status, `False` otherwise.
        """
        if not self.list_statuses:
            return False
        candidate_ids = target_list_ids or list(self.list_statuses.keys())
        for list_id in candidate_ids:
            status = self.list_statuses.get(list_id)
            if status and _is_blocked_status(status):
                return True
        return False

    def is_confirmed_for_all(self, target_list_ids: list[int]) -> bool:
        """
        Determine whether the subscriber is confirmed/active for every list in the given target_list_ids.
        
        If target_list_ids is empty, delegates to is_confirmed_for_any. If per-list statuses are not available, falls back to the subscriber-level status.
        
        Parameters:
        	target_list_ids (list[int]): List IDs to verify confirmation for; may be empty.
        
        Returns:
        	`true` if the subscriber is confirmed/active for every list in target_list_ids, `false` otherwise.
        """
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


class ListmonkSDKClient:
    """Thin wrapper over listmonk Python SDK."""

    def __init__(self, settings: Settings) -> None:
        """
        Initialize the Listmonk SDK client with configuration and reset its login state.
        
        Parameters:
            settings (Settings): Configuration object containing Listmonk connection details and credentials used by the client.
        """
        self._settings = settings
        self._logged_in = False

    async def _ensure_login(self) -> None:
        """
        Ensure the Listmonk SDK is imported and the client is authenticated.
        
        Attempts to import the listmonk package, configures its base URL, and logs in using stored settings; on success marks the client as logged in.
        
        Raises:
            ListmonkClientError: If the listmonk package is not installed or authentication fails.
        """
        if self._logged_in:
            return
        try:
            import listmonk  # type: ignore
        except ModuleNotFoundError as exc:
            raise ListmonkClientError("listmonk package is not installed") from exc

        await asyncio.to_thread(listmonk.set_url_base, self._settings.listmonk_url)
        ok = await asyncio.to_thread(
            listmonk.login,
            self._settings.listmonk_user,
            self._settings.listmonk_password,
        )
        if not ok:
            raise ListmonkClientError("Listmonk SDK login failed")
        self._logged_in = True

    async def get_subscriber_state(self, *, subscriber_id: int) -> SubscriberState | None:
        """
        Retrieve the normalized subscriber state for a given subscriber ID from Listmonk.
        
        Returns:
            SubscriberState: The subscriber's state including status, list IDs, and per-list statuses when available, or `None` if the subscriber was not found or has no status.
        
        Raises:
            ListmonkClientError: If the Listmonk SDK is not installed or cannot be initialized.
        """
        try:
            import listmonk  # type: ignore
        except ModuleNotFoundError as exc:
            raise ListmonkClientError("listmonk package is not installed") from exc

        await self._ensure_login()
        _safe_info(
            "listmonk_get_subscriber_state_request",
            subscriber_id=subscriber_id,
        )
        payload = await asyncio.to_thread(listmonk.subscriber_by_id, subscriber_id)
        if payload is None:
            _safe_info(
                "listmonk_get_subscriber_state_done",
                subscriber_id=subscriber_id,
                found=False,
            )
            return None

        status_raw = str(getattr(payload, "status", "") or "")
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
                    except (TypeError, ValueError):
                        continue
                    continue
                value = getattr(item, "id", None)
                if isinstance(value, int):
                    list_ids.append(value)

        if not status_raw:
            _safe_info(
                "listmonk_get_subscriber_state_done",
                subscriber_id=subscriber_id,
                found=False,
                reason="empty_status",
            )
            return None
        state = SubscriberState(
            subscriber_id=subscriber_id,
            status=status_raw,
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

    async def upsert_subscriber(
        self,
        *,
        email: str | None,
        list_ids: list[int],
        attributes: dict[str, object],
        subscriber_id: int | None = None,
    ) -> SubscriberState:
        """
        Ensure a subscriber with the provided data exists in Listmonk by creating it if no subscriber_id is given or updating the existing subscriber, and return the subscriber's normalized state.
        
        Parameters:
            email (str | None): Subscriber email. Required when creating a new subscriber (when `subscriber_id` is None).
            list_ids (list[int]): Target list IDs to assign the subscriber.
            attributes (dict[str, object]): Subscriber attributes; `fio` is used as the display name when present and all attributes are sent to Listmonk as `attribs`.
            subscriber_id (int | None): If provided, update the subscriber with this ID; if not found and `email` is provided, a new subscriber will be created.
        
        Returns:
            SubscriberState: Normalized subscriber state containing `subscriber_id`, `status`, `list_ids`, and optional per-list `list_statuses`.
        
        Raises:
            ListmonkClientError: If the Listmonk SDK is not installed, required data for creation is missing, the SDK response lacks an ID for a created subscriber, or a 409 conflict cannot be resolved by looking up the existing subscriber.
        """
        await self._ensure_login()
        if subscriber_id is None and not email:
            raise ListmonkClientError("Subscriber email is required to create subscriber")
        _safe_info(
            "listmonk_upsert_subscriber_request",
            subscriber_id=subscriber_id,
            email=email,
            list_ids=list_ids,
        )

        request_kwargs: dict[str, Any] = {
            "email": email,
            "name": attributes.get("fio") or email or "",
            "attribs": attributes,
            "subscriber_id": subscriber_id,
            "id": subscriber_id,
            "list_ids": list_ids,
        }
        if subscriber_id is not None:
            try:
                import listmonk  # type: ignore
            except ModuleNotFoundError as exc:
                raise ListmonkClientError("listmonk package is not installed") from exc
            subscriber = await asyncio.to_thread(listmonk.subscriber_by_id, subscriber_id)
            if subscriber is None:
                # Subscriber was removed from Listmonk, recreate by email.
                if not email:
                    raise ListmonkClientError(
                        f"subscriber_id={subscriber_id} not found and email is empty"
                    )
                return await self.upsert_subscriber(
                    email=email,
                    list_ids=list_ids,
                    attributes=attributes,
                    subscriber_id=None,
                )
            _apply_subscriber_update_fields(
                subscriber=subscriber,
                email=request_kwargs["email"],
                name=request_kwargs["name"],
                attribs=request_kwargs["attribs"],
            )
            current_status = getattr(subscriber, "status", None) or "enabled"
            response = await asyncio.to_thread(
                listmonk.update_subscriber,
                subscriber,
                set(list_ids),
                None,
                current_status,
            )
            state = await self.get_subscriber_state(subscriber_id=subscriber_id)
            if state is not None:
                _safe_info(
                    "listmonk_upsert_subscriber_done",
                    mode="update",
                    subscriber_id=state.subscriber_id,
                    status=state.status,
                    list_ids=state.list_ids,
                )
                return state
            status = _extract_status(response) or str(current_status)
            state = SubscriberState(subscriber_id=subscriber_id, status=status, list_ids=list_ids)
            _safe_info(
                "listmonk_upsert_subscriber_done",
                mode="update",
                subscriber_id=state.subscriber_id,
                status=state.status,
                list_ids=state.list_ids,
            )
            return state

        try:
            import listmonk  # type: ignore
        except ModuleNotFoundError as exc:
            raise ListmonkClientError("listmonk package is not installed") from exc
        try:
            response = await asyncio.to_thread(
                listmonk.create_subscriber,
                request_kwargs["email"],
                request_kwargs["name"],
                set(list_ids),
                False,
                request_kwargs["attribs"],
            )
        except Exception as exc:
            if not _is_conflict_error(exc) or not request_kwargs["email"]:
                raise
            # Idempotent behavior: subscriber already exists, switch to update by email.
            existing = await asyncio.to_thread(listmonk.subscriber_by_email, request_kwargs["email"])
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
            response = await asyncio.to_thread(
                listmonk.update_subscriber,
                existing,
                set(list_ids),
                None,
                getattr(existing, "status", None) or "enabled",
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
        state = SubscriberState(subscriber_id=created_id, status=status, list_ids=list_ids)
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
        """
        Ensure a subscriber exists for the given email and lists, and apply a restored subscription status when requested.
        
        If the subscriber does not exist, it will be created. The optional `desired_status` is normalized before applying:
        - "blocked" or "blacklisted" -> "blocklisted"
        - "confirmed" or "active" -> "enabled"
        - any other non-empty value -> "disabled"
        If `desired_status` is None or empty, the subscriber is left with its current status.
        
        Parameters:
            email (str | None): Subscriber email; required to create or restore a subscriber.
            list_ids (list[int]): IDs of lists to ensure the subscriber is associated with.
            attributes (dict[str, object] | None): Attributes to set on the subscriber when creating or updating.
            desired_status (str | None): Desired status to apply after ensuring the subscriber exists; normalized as described above.
        
        Returns:
            SubscriberState: The subscriber state after creation/update and after attempting to apply the desired status.
        
        Raises:
            ListmonkClientError: If `email` is not provided or if the listmonk SDK is not available.
        """
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
        subscriber = await asyncio.to_thread(listmonk.subscriber_by_id, state.subscriber_id)
        if subscriber is None:
            return state
        await asyncio.to_thread(
            listmonk.update_subscriber,
            subscriber,
            set(list_ids),
            None,
            target_status,
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
        await asyncio.to_thread(listmonk.delete_subscriber, None, subscriber_id)
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
        """
        Return incremental subscriber changes for a single list since a given watermark.
        
        Parameters:
            list_id (int): Identifier of the list to query.
            watermark_updated_at (datetime | None): Only include changes with updated_at greater than the watermark; if equal, watermark_subscriber_id breaks ties.
            watermark_subscriber_id (int | None): Subscriber ID used to break ties when updated_at equals watermark_updated_at; only include subscriber IDs greater than this when timestamps are equal.
            limit (int): Maximum number of deltas to return; if less than 1, at least one delta may still be returned.
        
        Returns:
            list[SubscriberDelta]: SubscriberDelta objects for changes after the watermark, sorted by (updated_at, subscriber_id) and truncated to the requested limit.
        """
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
        subscribers = await asyncio.to_thread(listmonk.subscribers, None, list_id)
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
    """
    Extracts a subscriber ID from various payload shapes returned by the Listmonk SDK.
    
    Parameters:
        payload (object): A response payload that may be a dict or an object with `id` or `subscriber_id` fields.
    
    Returns:
        int | None: The numeric subscriber ID if present (accepts integers or digit strings), otherwise `None`.
    """
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
    """
    Extract the status string from a payload if present.
    
    Parameters:
        payload (object): A dict or object that may contain a `status` attribute or key.
    
    Returns:
        The status string found in `payload`, or None if no string status is present.
    """
    if payload is None:
        return None
    if isinstance(payload, dict):
        value = payload.get("status")
        if isinstance(value, str):
            return value
    value = getattr(payload, "status", None)
    if isinstance(value, str):
        return value
    return None


def _extract_updated_at(payload: object) -> datetime:
    """
    Return the timestamp representing when the payload was last updated, in UTC.
    
    Checks for an `updated_at` datetime on the payload and returns it converted to UTC. If `updated_at` is missing or not a datetime, falls back to `created_at` converted to UTC. If neither is a datetime, returns `datetime.min` with UTC tzinfo.
    
    Returns:
    	datetime: The timestamp in UTC.
    """
    value = getattr(payload, "updated_at", None)
    if isinstance(value, datetime):
        return _to_utc(value)
    created = getattr(payload, "created_at", None)
    if isinstance(created, datetime):
        return _to_utc(created)
    return datetime.min.replace(tzinfo=UTC)


def _extract_list_ids(payload: object) -> list[int]:
    """
    Extract integer list IDs from a payload's `lists` attribute.
    
    Parameters:
        payload (object): Object that may have a `lists` attribute containing an iterable of items. Each item can be an int, a dict with an `"id"` key, or an object with an `id` attribute.
    
    Returns:
        list_ids (list[int]): List of extracted integer IDs. Entries that are missing, non-numeric, or cannot be converted to int are omitted.
    """
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
                except (TypeError, ValueError):
                    continue
                continue
            value = getattr(item, "id", None)
            if isinstance(value, int):
                list_ids.append(value)
    return list_ids


def _extract_list_statuses(payload: object) -> dict[int, str]:
    """
    Extract per-list subscription statuses from a Listmonk subscriber payload.
    
    Scans payload.lists (if present and iterable) for list entries and builds a mapping from list ID to the subscriber's status on that list. For each list entry the function:
    - accepts dicts or objects with attributes,
    - reads the list identifier from `id` (int or numeric string),
    - prefers `subscription_status` when present and non-empty, otherwise uses `status`.
    
    Parameters:
        payload (object): An object (typically a Listmonk subscriber payload) that may have a `lists` attribute containing list entries.
    
    Returns:
        dict[int, str]: A dictionary mapping list IDs to their corresponding subscription status. Returns an empty dict if no valid list/status pairs are found.
    """
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
    """
    Extract the email address from a subscriber payload.
    
    Parameters:
        payload (object): A mapping or object that may contain an `email` key or attribute.
    
    Returns:
        The email string if present and a string, otherwise None.
    """
    if isinstance(payload, dict):
        value = payload.get("email")
        if isinstance(value, str):
            return value
    value = getattr(payload, "email", None)
    if isinstance(value, str):
        return value
    return None


def _extract_attributes(payload: object) -> dict[str, Any] | None:
    """
    Extracts subscriber attributes from a Listmonk payload.
    
    Attempts to read a mapping of attributes under the keys "attribs" or "attributes" from either a dict payload or an object with those attributes. If found, returns a shallow copy of that mapping; otherwise returns None.
    
    Parameters:
    	payload (object): Payload returned by the Listmonk SDK or a dict-like payload that may contain subscriber attributes.
    
    Returns:
    	dict[str, Any] | None: A shallow copy of the attributes mapping if present, `None` if no attributes were found.
    """
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
    """
    Convert a datetime to UTC.
    
    If `value` is naive (tzinfo is None), returns the same datetime with UTC assigned as its tzinfo.
    If `value` is timezone-aware, returns a new datetime representing the same instant converted to UTC.
    
    Parameters:
        value (datetime): A naive or timezone-aware datetime.
    
    Returns:
        datetime: A datetime that is in UTC.
    """
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
    """
    Determine whether a subscriber record (identified by updated_at and subscriber_id) is strictly after the provided watermark.
    
    Parameters:
        updated_at (datetime): The record's last-modified timestamp (assumed UTC-aware or will be converted to UTC).
        subscriber_id (int): The record's numeric identifier used to break ties when timestamps are equal.
        watermark_updated_at (datetime | None): The watermark timestamp to compare against; when None, every record is considered after the watermark.
        watermark_subscriber_id (int | None): The watermark subscriber id used as a tie-breaker when timestamps are equal; treated as 0 if None.
    
    Returns:
        bool: `true` if `updated_at` is later than `watermark_updated_at`, or if equal and `subscriber_id` is greater than `watermark_subscriber_id`; `false` otherwise.
    """
    if watermark_updated_at is None:
        return True
    watermark_dt = _to_utc(watermark_updated_at)
    if updated_at > watermark_dt:
        return True
    if updated_at < watermark_dt:
        return False
    return subscriber_id > (watermark_subscriber_id or 0)


def _is_conflict_error(exc: Exception) -> bool:
    """
    Determine whether an exception represents an HTTP 409 Conflict error.
    
    Checks the exception's `response.status_code` (if present) for HTTP 409, or falls back to inspecting the exception text for both "409" and "conflict".
    
    Returns:
        bool: `True` if the exception indicates an HTTP 409 Conflict, `False` otherwise.
    """
    response = getattr(exc, "response", None)
    if response is not None and getattr(response, "status_code", None) == httpx.codes.CONFLICT:
        return True
    text = str(exc).lower()
    return "409" in text and "conflict" in text


def _normalize_status_for_restore(status: str | None) -> str | None:
    """
    Normalize a desired restore status to Listmonk's canonical status values.
    
    Parameters:
        status (str | None): The requested status to normalize; may be None or contain surrounding whitespace and case variations.
    
    Returns:
        str | None: `None` if input is None or empty; `"blocklisted"` for blocked/blacklisted inputs; `"enabled"` for confirmed/active inputs; `"disabled"` for any other non-empty input.
    """
    if status is None:
        return None
    normalized = status.strip().lower()
    if not normalized:
        return None
    if normalized in {"blocked", "blacklisted"}:
        return "blocklisted"
    # Only explicitly confirmed/active should be restored as confirmed (enabled).
    if normalized in {"confirmed", "active"}:
        return "enabled"
    # Any other non-blocked status is restored as non-confirmed.
    return "disabled"


def _apply_subscriber_update_fields(
    *,
    subscriber: object,
    email: str | None,
    name: str,
    attribs: dict[str, object],
) -> None:
    """
    Attempt to update a subscriber representation in place by setting its email, name, and attribs.
    
    This is a best-effort mutator that supports either a dict (updates keys "email", "name", "attribs") or an object with writable attributes of the same names. If the subscriber object is immutable or assignments fail, the function returns silently without raising.
    
    Parameters:
        subscriber (object): Subscriber payload as a dict or SDK subscriber object to mutate.
        email (str | None): Email value to assign; may be None.
        name (str): Name value to assign.
        attribs (dict[str, object]): Attributes dictionary to assign to the subscriber.
    """
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
    """
    Determine whether a subscriber status represents a blocked state.
    
    Parameters:
    	status (str): Status string to evaluate; surrounding whitespace and case are ignored.
    
    Returns:
    	bool: `True` if `status` is "blocked", "blocklisted", or "blacklisted" (case-insensitive, ignoring surrounding whitespace), `False` otherwise.
    """
    return status.strip().lower() in {"blocked", "blocklisted", "blacklisted"}


def _is_confirmed_status(status: str) -> bool:
    """
    Check whether the given subscriber status represents a confirmed or active state.
    
    Parameters:
        status (str): Status string to evaluate; leading/trailing whitespace is ignored and comparison is case-insensitive.
    
    Returns:
        `true` if `status` is "enabled", "confirmed", or "active"; `false` otherwise.
    """
    return status.strip().lower() in {"enabled", "confirmed", "active"}
