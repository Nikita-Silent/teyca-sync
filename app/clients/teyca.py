"""Async Teyca API client."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from time import monotonic
from uuid import uuid4

import httpx
import structlog

from app.config import Settings

logger = structlog.get_logger()


class TeycaAPIError(Exception):
    """Raised when Teyca API call fails."""


@dataclass(slots=True)
class BonusOperation:
    """Single bonus operation payload for Teyca bonuses API."""

    value: str

    def to_dict(self) -> dict[str, str]:
        return {"value": self.value}

    @staticmethod
    def one_shot(value: str) -> BonusOperation:
        """Create operation payload with a single value."""
        return BonusOperation(value=value)


class SlidingWindowRateLimiter:
    """Async sliding-window rate limiter for multiple windows."""

    def __init__(
        self,
        *,
        limits: tuple[tuple[float, int], ...],
        clock: Callable[[], float] = monotonic,
    ) -> None:
        self._limits = limits
        self._clock = clock
        self._lock = asyncio.Lock()
        self._requests_by_window: dict[float, deque[float]] = {
            window_seconds: deque() for window_seconds, _ in limits
        }

    async def acquire(self) -> None:
        """Wait until request can be executed for all configured windows."""
        while True:
            wait_seconds: float = 0.0
            async with self._lock:
                now = self._clock()
                for window_seconds, max_requests in self._limits:
                    requests = self._requests_by_window[window_seconds]
                    cutoff = now - window_seconds
                    while requests and requests[0] <= cutoff:
                        requests.popleft()

                    if len(requests) >= max_requests:
                        wait_seconds = max(wait_seconds, (requests[0] + window_seconds) - now)

                if wait_seconds <= 0:
                    for requests in self._requests_by_window.values():
                        requests.append(now)
                    return

            logger.info("teyca_rate_limited", wait_seconds=round(wait_seconds, 3))
            await asyncio.sleep(wait_seconds)


class TeycaClient:
    """HTTP client for Teyca bonuses endpoints."""

    _DEFAULT_LIMITS: tuple[tuple[float, int], ...] = (
        (1.0, 5),
        (60.0, 150),
        (3600.0, 1000),
        (86400.0, 5000),
    )

    def __init__(
        self,
        settings: Settings,
        http_client: httpx.AsyncClient | None = None,
        rate_limiter: SlidingWindowRateLimiter | None = None,
    ) -> None:
        self._settings = settings
        self._client = http_client
        self._rate_limiter = rate_limiter or SlidingWindowRateLimiter(limits=self._DEFAULT_LIMITS)

    async def accrue_bonuses(self, *, user_id: int, bonuses: list[BonusOperation]) -> None:
        """Call POST /v1/{token}/passes/{user_id}/bonuses."""
        headers = self._get_headers()
        url = f"{self._get_pass_url(user_id=user_id)}/bonuses"
        payload = {"bonus": [item.to_dict() for item in bonuses]}
        request_id = str(uuid4())
        logger.info(
            "teyca_accrue_bonuses_request",
            request_id=request_id,
            user_id=user_id,
            url=url,
            operation_count=len(bonuses),
            payload=payload,
        )
        await self._rate_limiter.acquire()

        if self._client is None:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(url, json=payload, headers=headers)
        else:
            response = await self._client.post(url, json=payload, headers=headers)

        if response.status_code >= 400:
            logger.error(
                "teyca_accrue_bonuses_failed",
                request_id=request_id,
                user_id=user_id,
                url=url,
                payload=payload,
                status_code=response.status_code,
                response_body=response.text,
            )
            raise TeycaAPIError(
                f"Teyca bonuses request failed: status={response.status_code}, body={response.text}"
            )
        logger.info(
            "teyca_accrue_bonuses_done",
            request_id=request_id,
            user_id=user_id,
            url=url,
            payload=payload,
            status_code=response.status_code,
        )

    async def update_pass_fields(self, *, user_id: int, fields: dict[str, object]) -> None:
        """Call PUT /v1/{token}/passes/{user_id} with partial fields."""
        headers = self._get_headers()
        url = self._get_pass_url(user_id=user_id)
        request_id = str(uuid4())
        logger.info(
            "teyca_update_pass_request",
            request_id=request_id,
            user_id=user_id,
            url=url,
            partial=True,
            field_names=sorted(str(key) for key in fields.keys()),
            fields=fields,
        )
        await self._rate_limiter.acquire()

        if self._client is None:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.put(url, json=fields, headers=headers)
        else:
            response = await self._client.put(url, json=fields, headers=headers)

        if response.status_code >= 400:
            logger.error(
                "teyca_update_pass_failed",
                request_id=request_id,
                user_id=user_id,
                url=url,
                partial=True,
                field_names=sorted(str(key) for key in fields.keys()),
                fields=fields,
                status_code=response.status_code,
                response_body=response.text,
            )
            raise TeycaAPIError(
                f"Teyca pass update failed: status={response.status_code}, body={response.text}"
            )
        logger.info(
            "teyca_update_pass_done",
            request_id=request_id,
            user_id=user_id,
            url=url,
            partial=True,
            field_names=sorted(str(key) for key in fields.keys()),
            fields=fields,
            status_code=response.status_code,
        )

    def _get_headers(self) -> dict[str, str]:
        if not self._settings.teyca_token or not self._settings.teyca_api_key:
            raise TeycaAPIError("TEYCA_TOKEN/TEYCA_API_KEY are not configured")
        return {"Authorization": self._settings.teyca_api_key}

    def _get_pass_url(self, *, user_id: int) -> str:
        return (
            f"{self._settings.teyca_base_url.rstrip('/')}"
            f"/v1/{self._settings.teyca_token}/passes/{user_id}"
        )
