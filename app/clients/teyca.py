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
        """
        Convert the BonusOperation to a dictionary suitable for the Teyca API payload.
        
        Returns:
            dict[str, str]: A dictionary with a single key "value" mapped to the operation's value.
        """
        return {"value": self.value}

    @staticmethod
    def one_shot(value: str) -> "BonusOperation":
        """
        Create a BonusOperation representing a one-shot bonus with the provided value.
        
        Parameters:
        	value (str): The bonus value to include in the operation payload.
        
        Returns:
        	bonus_operation (BonusOperation): A BonusOperation whose `value` is set to the provided string.
        """
        return BonusOperation(value=value)


class TeycaClient:
    """HTTP client for Teyca bonuses endpoints."""

    def __init__(self, settings: Settings, http_client: httpx.AsyncClient | None = None) -> None:
        """
        Create a TeycaClient configured with application settings and an optional HTTP client.
        
        Parameters:
            settings (Settings): Application settings containing Teyca configuration (e.g., token, API key, and base URL).
            http_client (httpx.AsyncClient | None): Optional shared AsyncClient to use for requests; if omitted, a new client will be created per call.
        """
        self._settings = settings
        self._client = http_client

    async def accrue_bonuses(self, *, user_id: int, bonuses: list[BonusOperation]) -> None:
        """
        Accrues the provided bonus operations to the specified user's pass in Teyca.
        
        Parameters:
            user_id (int): Identifier of the user whose pass will receive the bonuses.
            bonuses (list[BonusOperation]): BonusOperation instances describing the bonuses to apply.
        
        Raises:
            TeycaAPIError: If the Teyca API responds with an error status or a request fails.
        """
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
        """
        Update partial fields of a user's pass in Teyca.
        
        Sends a PUT request to the pass endpoint for the specified user with the supplied fields.
        Raises TeycaAPIError if the Teyca API returns an HTTP status code >= 400.
        
        Parameters:
            user_id (int): Identifier of the user whose pass will be updated.
            fields (dict[str, object]): Mapping of pass field names to new values to apply.
        """
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
        """
        Builds the HTTP headers required to authenticate requests to the Teyca API.
        
        Returns:
            headers (dict[str, str]): Dictionary containing an "Authorization" header set to the configured API key.
        
        Raises:
            TeycaAPIError: If either `teyca_token` or `teyca_api_key` is not configured in settings.
        """
        if not self._settings.teyca_token or not self._settings.teyca_api_key:
            raise TeycaAPIError("TEYCA_TOKEN/TEYCA_API_KEY are not configured")
        return {"Authorization": self._settings.teyca_api_key}

    def _get_pass_url(self, *, user_id: int) -> str:
        """
        Build the Teyca pass endpoint URL for the given user.
        
        Returns:
            The full passes endpoint URL composed from the configured Teyca base URL and token targeting the specified user ID.
        """
        return (
            f"{self._settings.teyca_base_url.rstrip('/')}"
            f"/v1/{self._settings.teyca_token}/passes/{user_id}"
        )
