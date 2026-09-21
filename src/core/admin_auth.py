import secrets

from fastapi import Header, HTTPException, status

from src.core.config import settings


def require_admin_api_key(x_admin_key: str | None = Header(default=None)) -> None:
    """Fail closed when administrative authentication is not configured."""
    configured_key = settings.ADMIN_API_KEY
    if not configured_key:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Administrative API is not configured",
        )
    if not x_admin_key or not secrets.compare_digest(x_admin_key, configured_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid administrative credentials",
            headers={"WWW-Authenticate": "ApiKey"},
        )
