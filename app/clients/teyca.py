"""Async Teyca API client."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from hashlib import sha1
from time import monotonic
from typing import Any, Protocol, cast
from uuid import uuid4

import httpx
import structlog
from redis.asyncio import Redis

from app.config import Settings

logger = structlog.get_logger()


class TeycaAPIError(Exception):
    """Raised when Teyca API call fails."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code

    @property
    def is_rate_limited(self) -> bool:
        """Return True when Teyca rejected the request with HTTP 429."""
        return self.status_code == 429


class TeycaRateLimitBusyError(TeycaAPIError):
    """Raised when a caller refuses to wait for the shared Teyca limiter."""

    def __init__(
        self,
        *,
        wait_seconds: float,
        max_wait_seconds: float,
        backend: str,
    ) -> None:
        super().__init__(
            
                "Teyca rate limiter is busy: "
                f"backend={backend}, wait_seconds={wait_seconds:.3f}, "
                f"max_wait_seconds={max_wait_seconds:.3f}"
            
        )
        self.wait_seconds = wait_seconds
        self.max_wait_seconds = max_wait_seconds
        self.backend = backend


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


class RateLimiter(Protocol):
    """Minimal async contract shared by local and Redis limiters."""

    async def acquire(self, *, max_wait_seconds: float | None = None) -> None:
        """Wait until a request slot becomes available."""


class AsyncRedisEvalClient(Protocol):
    """Minimal Redis contract required by the distributed limiter."""

    async def eval(self, script: str, numkeys: int, *keys_and_args: str) -> Any:
        """Execute the limiter Lua script."""


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

    async def acquire(self, *, max_wait_seconds: float | None = None) -> None:
        """Wait until request can be executed for all configured windows."""
        deadline: float | None = None
        if max_wait_seconds is not None:
            deadline = self._clock() + max(0.0, max_wait_seconds)

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

                if deadline is not None:
                    remaining_seconds = max(0.0, deadline - now)
                    if wait_seconds > remaining_seconds:
                        logger.info(
                            "teyca_rate_limiter_busy",
                            backend="local",
                            wait_seconds=round(wait_seconds, 3),
                            max_wait_seconds=round(max_wait_seconds or 0.0, 3),
                        )
                        raise TeycaRateLimitBusyError(
                            wait_seconds=wait_seconds,
                            max_wait_seconds=max_wait_seconds or 0.0,
                            backend="local",
                        )

            logger.info("teyca_rate_limited", wait_seconds=round(wait_seconds, 3))
            await asyncio.sleep(wait_seconds)


class RedisSlidingWindowRateLimiter:
    """Distributed sliding-window limiter backed by Redis sorted sets."""

    _ACQUIRE_SCRIPT = """
local request_id = ARGV[1]
local pair_count = tonumber(ARGV[2])
local time_result = redis.call("TIME")
local now_ms = (tonumber(time_result[1]) * 1000) + math.floor(tonumber(time_result[2]) / 1000)
local wait_ms = 0

for index = 1, pair_count do
    local key = KEYS[index]
    local arg_offset = 2 + ((index - 1) * 2)
    local window_ms = tonumber(ARGV[arg_offset + 1])
    local max_requests = tonumber(ARGV[arg_offset + 2])
    local cutoff = now_ms - window_ms
    redis.call("ZREMRANGEBYSCORE", key, 0, cutoff)

    local current = redis.call("ZCARD", key)
    if current >= max_requests then
        local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
        if oldest[2] ~= nil then
            local candidate_wait_ms = math.ceil((tonumber(oldest[2]) + window_ms) - now_ms)
            if candidate_wait_ms > wait_ms then
                wait_ms = candidate_wait_ms
            end
        end
    end
end

if wait_ms > 0 then
    return {0, wait_ms}
end

for index = 1, pair_count do
    local key = KEYS[index]
    local arg_offset = 2 + ((index - 1) * 2)
    local window_ms = tonumber(ARGV[arg_offset + 1])
    redis.call("ZADD", key, now_ms, request_id)
    redis.call("PEXPIRE", key, window_ms + 60000)
end

return {1, 0}
"""

    def __init__(
        self,
        *,
        redis_client: AsyncRedisEvalClient,
        limits: tuple[tuple[float, int], ...],
        key_prefix: str,
        sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
        min_sleep_seconds: float = 0.05,
        request_id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._redis = redis_client
        self._limits = limits
        self._key_prefix = key_prefix
        self._sleep = sleep
        self._min_sleep_seconds = min_sleep_seconds
        self._request_id_factory = request_id_factory or (lambda: uuid4().hex)

    async def acquire(self, *, max_wait_seconds: float | None = None) -> None:
        """Wait until a shared request slot becomes available in Redis."""
        keys = [
            f"{self._key_prefix}:window:{int(window_seconds * 1000)}"
            for window_seconds, _ in self._limits
        ]
        args: list[str] = [self._request_id_factory(), str(len(self._limits))]
        for window_seconds, max_requests in self._limits:
            args.extend((str(int(window_seconds * 1000)), str(max_requests)))

        deadline: float | None = None
        if max_wait_seconds is not None:
            deadline = monotonic() + max(0.0, max_wait_seconds)

        while True:
            try:
                result = await self._redis.eval(self._ACQUIRE_SCRIPT, len(keys), *keys, *args)
            except Exception:
                logger.exception(
                    "teyca_rate_limiter_redis_failed",
                    key_prefix=self._key_prefix,
                )
                raise

            allowed = int(result[0])
            wait_ms = max(0, int(result[1]))
            if allowed == 1:
                return

            wait_seconds = max(wait_ms / 1000.0, self._min_sleep_seconds)
            if deadline is not None:
                remaining_seconds = max(0.0, deadline - monotonic())
                if wait_seconds > remaining_seconds:
                    logger.info(
                        "teyca_rate_limiter_busy",
                        backend="redis",
                        wait_seconds=round(wait_seconds, 3),
                        max_wait_seconds=round(max_wait_seconds or 0.0, 3),
                    )
                    raise TeycaRateLimitBusyError(
                        wait_seconds=wait_seconds,
                        max_wait_seconds=max_wait_seconds or 0.0,
                        backend="redis",
                    )
            logger.info(
                "teyca_rate_limited",
                backend="redis",
                wait_seconds=round(wait_seconds, 3),
            )
            await self._sleep(wait_seconds)


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
        rate_limiter: RateLimiter,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._settings = settings
        self._client = http_client
        self._rate_limiter = rate_limiter

    async def accrue_bonuses(
        self,
        *,
        user_id: int,
        bonuses: list[BonusOperation],
        rate_limit_max_wait_seconds: float | None = None,
    ) -> None:
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
        await self._rate_limiter.acquire(max_wait_seconds=rate_limit_max_wait_seconds)

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
                (
                    "Teyca bonuses request failed: "
                    f"status={response.status_code}, body={response.text}"
                ),
                status_code=response.status_code,
            )
        logger.info(
            "teyca_accrue_bonuses_done",
            request_id=request_id,
            user_id=user_id,
            url=url,
            payload=payload,
            status_code=response.status_code,
        )

    async def update_pass_fields(
        self,
        *,
        user_id: int,
        fields: dict[str, object],
        rate_limit_max_wait_seconds: float | None = None,
    ) -> None:
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
        await self._rate_limiter.acquire(max_wait_seconds=rate_limit_max_wait_seconds)

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
                (f"Teyca pass update failed: status={response.status_code}, body={response.text}"),
                status_code=response.status_code,
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


def build_teyca_rate_limiter(settings: Settings) -> RateLimiter:
    """Create a shared Redis-backed limiter or an explicitly allowed local one."""
    redis_url = str(getattr(settings, "teyca_rate_limit_redis_url", "") or "").strip()
    if redis_url:
        redis_client = cast(AsyncRedisEvalClient, Redis.from_url(redis_url))
        return RedisSlidingWindowRateLimiter(
            redis_client=redis_client,
            limits=TeycaClient._DEFAULT_LIMITS,
            key_prefix=_build_redis_key_prefix(settings),
        )

    allow_local = bool(getattr(settings, "teyca_allow_local_rate_limiter", False))
    if not allow_local:
        raise TeycaAPIError(
            "TEYCA_RATE_LIMIT_REDIS_URL must be configured unless "
            "TEYCA_ALLOW_LOCAL_RATE_LIMITER=true is set explicitly"
        )

    logger.warning("teyca_rate_limiter_local_fallback_enabled")
    return SlidingWindowRateLimiter(limits=TeycaClient._DEFAULT_LIMITS)


def build_teyca_client(settings: Settings) -> TeycaClient:
    """Build Teyca client with an explicit limiter selection."""
    return TeycaClient(
        settings=settings,
        rate_limiter=build_teyca_rate_limiter(settings),
    )


def _build_redis_key_prefix(settings: Settings) -> str:
    """Scope limiter keys to one configured Teyca account."""
    configured_prefix = (
        str(getattr(settings, "teyca_rate_limit_redis_prefix", "teyca-rate-limit") or "").strip()
        or "teyca-rate-limit"
    )
    raw_namespace = (
        f"{str(getattr(settings, 'teyca_base_url', '')).rstrip('/')}"
        f"|{str(getattr(settings, 'teyca_token', ''))}"
    )
    namespace_hash = sha1(raw_namespace.encode("utf-8")).hexdigest()[:12]
    return f"{configured_prefix}:{namespace_hash}"
