"""Async Teyca API client."""

from __future__ import annotations

from dataclasses import dataclass

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
    def one_shot(value: str) -> "BonusOperation":
        """Create operation payload with a single value."""
        return BonusOperation(value=value)


class TeycaClient:
    """HTTP client for Teyca bonuses endpoints."""

    def __init__(self, settings: Settings, http_client: httpx.AsyncClient | None = None) -> None:
        self._settings = settings
        self._client = http_client

    async def accrue_bonuses(self, *, user_id: int, bonuses: list[BonusOperation]) -> None:
        """Call POST /v1/{token}/passes/{user_id}/bonuses."""
        headers = self._get_headers()
        url = f"{self._get_pass_url(user_id=user_id)}/bonuses"
        payload = {"bonus": [item.to_dict() for item in bonuses]}
        logger.info(
            "teyca_accrue_bonuses_request",
            user_id=user_id,
            url=url,
            operation_count=len(bonuses),
        )

        if self._client is None:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(url, json=payload, headers=headers)
        else:
            response = await self._client.post(url, json=payload, headers=headers)

        if response.status_code >= 400:
            logger.error(
                "teyca_accrue_bonuses_failed",
                user_id=user_id,
                url=url,
                status_code=response.status_code,
                response_body=response.text,
            )
            raise TeycaAPIError(
                f"Teyca bonuses request failed: status={response.status_code}, body={response.text}"
            )
        logger.info(
            "teyca_accrue_bonuses_done",
            user_id=user_id,
            url=url,
            status_code=response.status_code,
        )

    async def update_pass_fields(self, *, user_id: int, fields: dict[str, object]) -> None:
        """Call PUT /v1/{token}/passes/{user_id} with partial fields."""
        headers = self._get_headers()
        url = self._get_pass_url(user_id=user_id)
        logger.info(
            "teyca_update_pass_request",
            user_id=user_id,
            url=url,
            field_names=sorted(str(key) for key in fields.keys()),
        )

        if self._client is None:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.put(url, json=fields, headers=headers)
        else:
            response = await self._client.put(url, json=fields, headers=headers)

        if response.status_code >= 400:
            logger.error(
                "teyca_update_pass_failed",
                user_id=user_id,
                url=url,
                status_code=response.status_code,
                response_body=response.text,
            )
            raise TeycaAPIError(
                f"Teyca pass update failed: status={response.status_code}, body={response.text}"
            )
        logger.info(
            "teyca_update_pass_done",
            user_id=user_id,
            url=url,
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
