"""Webhook auth: static token in Authorization header (Teyca sends a fixed secret)."""

from fastapi import Header, HTTPException

from app.config import get_settings


async def verify_webhook_token(
    authorization: str | None = Header(default=None),
) -> None:
    """
    Validate the Authorization header against the configured webhook token.
    
    Accepts an Authorization header value (either "Bearer <token>" or a raw token) and raises an HTTPException if the application is not configured for webhook authentication, the header is missing, or the token does not match the configured value.
    
    Parameters:
        authorization (str | None): The raw Authorization header value from the request.
    
    Raises:
        HTTPException: with status 503 and detail "Webhook auth not configured" if no token is configured.
        HTTPException: with status 401 and detail "Missing Authorization" if the Authorization header is absent.
        HTTPException: with status 403 and detail "Invalid Authorization" if the provided token does not match the configured token.
    """
    settings = get_settings()
    if not settings.webhook_auth_token:
        raise HTTPException(status_code=503, detail="Webhook auth not configured")
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization")
    raw = authorization.strip()
    token_only = raw.removeprefix("Bearer ").strip() if raw.lower().startswith("bearer ") else raw
    if token_only != settings.webhook_auth_token:
        raise HTTPException(status_code=403, detail="Invalid Authorization")
    return None
