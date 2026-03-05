"""Listmonk client abstraction based on Python SDK."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from app.config import Settings


class ListmonkClientError(Exception):
    """Raised when listmonk SDK is unavailable or returns unexpected payload."""


@dataclass(slots=True)
class SubscriberState:
    """Normalized subscriber state from Listmonk."""

    subscriber_id: int
    status: str
    list_ids: list[int]

    def is_confirmed_for_any(self, target_list_ids: list[int]) -> bool:
        """True when subscriber is active/confirmed in target list."""
        normalized = self.status.strip().lower()
        if normalized not in {"enabled", "confirmed", "active"}:
            return False
        if not target_list_ids:
            return True
        return any(list_id in target_list_ids for list_id in self.list_ids)


class ListmonkSDKClient:
    """Thin wrapper over listmonk Python SDK."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._logged_in = False

    async def _ensure_login(self) -> None:
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
        """Fetch subscriber state from Listmonk SDK."""
        try:
            import listmonk  # type: ignore
        except ModuleNotFoundError as exc:
            raise ListmonkClientError("listmonk package is not installed") from exc

        await self._ensure_login()
        payload = await asyncio.to_thread(listmonk.subscriber_by_id, subscriber_id)
        if payload is None:
            return None

        status_raw = str(getattr(payload, "status", "") or "")
        lists_raw = getattr(payload, "lists", [])
        list_ids: list[int] = []
        if isinstance(lists_raw, list):
            for item in lists_raw:
                if isinstance(item, dict) and "id" in item:
                    try:
                        list_ids.append(int(item["id"]))
                    except (TypeError, ValueError):
                        continue

        if not status_raw:
            return None
        return SubscriberState(
            subscriber_id=subscriber_id,
            status=status_raw,
            list_ids=list_ids,
        )
