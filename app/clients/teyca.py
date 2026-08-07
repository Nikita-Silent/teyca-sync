"""Async Teyca API client."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import uuid4

import httpx
import structlog

from app.config import Settings
from app.repositories.teyca_call_budget import BudgetLimits, TeycaCallBudgetRepository

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
    """Minimal async contract for the Teyca call limiter."""

    async def acquire(self, *, max_wait_seconds: float | None = None) -> None:
        """Reserve one call slot, or raise TeycaRateLimitBusyError immediately."""


class AsyncSessionFactory(Protocol):
    """Minimal contract for an async_sessionmaker-like session factory."""

    def __call__(self) -> Any:
        """Open a new async session as a context manager."""


class PostgresCallBudgetLimiter:
    """Non-blocking budget limiter backed by Postgres (teyca-sync-3al).

    Replaces the old Redis/local sliding-window limiters, which turned an
    exhausted window into a blocking wait (`wait_seconds=70839` was observed
    for a stuck daily-window task). This limiter never sleeps: exhausting any
    configured window raises `TeycaRateLimitBusyError` right away so the
    caller can defer and move on. The budget lives in `teyca_call_budget`, so
    it survives restarts, is shared across worker processes, and is visible
    with a plain SQL query.
    """

    def __init__(
        self,
        *,
        session_factory: AsyncSessionFactory,
        limits: BudgetLimits,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._limits = limits
        self._clock = clock or (lambda: datetime.now(UTC))

    async def acquire(self, *, max_wait_seconds: float | None = None) -> None:
        """Reserve one call slot across every configured window, or fail fast."""
        now = self._clock()
        async with self._session_factory() as session:
            repo = TeycaCallBudgetRepository(session)
            try:
                reservation = await repo.try_reserve(limits=self._limits, now=now)
            except Exception:
                await session.rollback()
                raise
            await session.commit()

        if reservation.allowed:
            return

        logger.info(
            "teyca_rate_limiter_busy",
            backend="postgres",
            wait_seconds=round(reservation.retry_after_seconds, 3),
            max_wait_seconds=round(max_wait_seconds or 0.0, 3),
        )
        raise TeycaRateLimitBusyError(
            wait_seconds=reservation.retry_after_seconds,
            max_wait_seconds=max_wait_seconds or 0.0,
            backend="postgres",
        )


def _build_budget_limits(settings: Settings) -> BudgetLimits:
    """Read configured Teyca limits (defaults: 5/s, 50/min, 500/hour, 5000/day)."""
    return (
        ("second", 1, int(getattr(settings, "teyca_rate_limit_per_second", 5))),
        ("minute", 60, int(getattr(settings, "teyca_rate_limit_per_minute", 50))),
        ("hour", 3600, int(getattr(settings, "teyca_rate_limit_per_hour", 500))),
        ("day", 86400, int(getattr(settings, "teyca_rate_limit_per_day", 5000))),
    )


class TeycaClient:
    """HTTP client for Teyca bonuses endpoints."""

    def __init__(
        self,
        settings: Settings,
        rate_limiter: RateLimiter,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._settings = settings
        self._client = http_client
        self._owns_client = http_client is None
        self._rate_limiter = rate_limiter

    def _get_client(self) -> httpx.AsyncClient:
        """Lazily create and reuse a client with connect/read/write/pool timeouts."""
        if self._client is None:
            timeout = httpx.Timeout(
                connect=float(getattr(self._settings, "teyca_connect_timeout_seconds", 5.0)),
                read=float(getattr(self._settings, "teyca_read_timeout_seconds", 15.0)),
                write=float(getattr(self._settings, "teyca_write_timeout_seconds", 15.0)),
                pool=float(getattr(self._settings, "teyca_pool_timeout_seconds", 5.0)),
            )
            self._client = httpx.AsyncClient(timeout=timeout)
        return self._client

    async def close(self) -> None:
        """Close the underlying HTTP client if this instance created it."""
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _execute(
        self,
        method: str,
        url: str,
        *,
        json: dict[str, Any],
        headers: dict[str, str],
        action: str,
    ) -> httpx.Response:
        """Execute an HTTP request with retry for transient errors and 5xx responses."""
        max_retries = max(0, int(getattr(self._settings, "teyca_request_max_retries", 2)))
        backoff = max(
            0.0,
            float(getattr(self._settings, "teyca_request_retry_backoff_seconds", 1.0)),
        )
        attempt = 0
        while True:
            try:
                client = self._get_client()
                response = await getattr(client, method)(url, json=json, headers=headers)
            except (httpx.TimeoutException, httpx.NetworkError, httpx.TransportError) as exc:
                if attempt >= max_retries:
                    raise
                logger.warning(
                    "teyca_request_retry",
                    action=action,
                    url=url,
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                attempt += 1
                if backoff > 0:
                    await asyncio.sleep(backoff * attempt)
                continue

            if response.status_code >= 500 and attempt < max_retries:
                logger.warning(
                    "teyca_request_retry",
                    action=action,
                    url=url,
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    status_code=response.status_code,
                )
                attempt += 1
                if backoff > 0:
                    await asyncio.sleep(backoff * attempt)
                continue

            return response

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
        response = await self._execute(
            "post",
            url,
            json=payload,
            headers=headers,
            action="accrue_bonuses",
        )

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
        response = await self._execute(
            "put",
            url,
            json=fields,
            headers=headers,
            action="update_pass_fields",
        )

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

    async def list_operations(
        self,
        *,
        user_ids: list[int],
        limit: int = 100,
        offset: int = 0,
        rate_limit_max_wait_seconds: float | None = None,
    ) -> list[dict[str, Any]]:
        """Call POST /v1/{token}/operations filtered by user_ids[] (batch history read).

        Used before one-off backdated accruals (teyca-sync-io3) to cross-check that
        Teyca does not already show a matching operation our own bonus_accrual_log
        missed — the log is the authoritative idempotency guard, this is a secondary
        sanity check against out-of-band grants.
        """
        if not user_ids:
            return []
        headers = self._get_headers()
        url = (
            f"{self._settings.teyca_base_url.rstrip('/')}"
            f"/v1/{self._settings.teyca_token}/operations?limit={limit}&offset={offset}"
        )
        payload: dict[str, Any] = {"filters": {"user_ids": list(user_ids)}, "order": "desc"}
        request_id = str(uuid4())
        logger.info(
            "teyca_list_operations_request",
            request_id=request_id,
            url=url,
            user_id_count=len(user_ids),
        )
        await self._rate_limiter.acquire(max_wait_seconds=rate_limit_max_wait_seconds)
        response = await self._execute(
            "post",
            url,
            json=payload,
            headers=headers,
            action="list_operations",
        )

        if response.status_code >= 400:
            logger.error(
                "teyca_list_operations_failed",
                request_id=request_id,
                url=url,
                status_code=response.status_code,
                response_body=response.text,
            )
            raise TeycaAPIError(
                (
                    "Teyca operations request failed: "
                    f"status={response.status_code}, body={response.text}"
                ),
                status_code=response.status_code,
            )
        items = _extract_operation_items(response.json())
        logger.info(
            "teyca_list_operations_done",
            request_id=request_id,
            url=url,
            status_code=response.status_code,
            item_count=len(items),
        )
        return items

    def _get_headers(self) -> dict[str, str]:
        if not self._settings.teyca_token or not self._settings.teyca_api_key:
            raise TeycaAPIError("TEYCA_TOKEN/TEYCA_API_KEY are not configured")
        return {"Authorization": self._settings.teyca_api_key}

    def _get_pass_url(self, *, user_id: int) -> str:
        return (
            f"{self._settings.teyca_base_url.rstrip('/')}"
            f"/v1/{self._settings.teyca_token}/passes/{user_id}"
        )


def _extract_operation_items(data: object) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        for key in ("items", "data", "results", "operations"):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def build_teyca_rate_limiter(
    settings: Settings, *, session_factory: AsyncSessionFactory
) -> RateLimiter:
    """Create the shared Postgres-backed call budget limiter (teyca-sync-3al)."""
    return PostgresCallBudgetLimiter(
        session_factory=session_factory,
        limits=_build_budget_limits(settings),
    )


def build_teyca_client(settings: Settings, *, session_factory: AsyncSessionFactory) -> TeycaClient:
    """Build Teyca client with the Postgres-backed rate limiter."""
    return TeycaClient(
        settings=settings,
        rate_limiter=build_teyca_rate_limiter(settings, session_factory=session_factory),
    )
