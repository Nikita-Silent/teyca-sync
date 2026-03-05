"""Webhook auth: static token in Authorization header (Teyca sends a fixed secret)."""

from fastapi import Header, HTTPException

from app.config import get_settings


async def verify_webhook_token(
    authorization: str | None = Header(default=None),
) -> None:
    """Accept request only if Authorization matches WEBHOOK_AUTH_TOKEN. 401 if missing, 403 if wrong."""
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
