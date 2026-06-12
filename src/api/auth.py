"""Admin authentication for the reset HTTP surface.

Mirrors the isnad-graph admin restriction so the *same* user-service RS256
JWT (validated against user-service's JWKS) authorizes a reset here as on the
isnad-graph admin dashboard. The reset endpoints are destructive, so the
guard is admin-only — a valid non-admin token is rejected with 403.

This is a deliberately slimmed copy of isnad-graph's
``src/api/middleware.py`` + ``src/api/auth.py`` auth path: JWKS fetch with a
TTL cache, RS256 verification with a single key-rotation retry, role
resolution from the ``roles`` claim, and a ``require_admin`` FastAPI
dependency. Kept self-contained (no shared package) because the two services
are independent repos.
"""

from __future__ import annotations

import logging
import time
from enum import StrEnum

import httpx
from fastapi import HTTPException, Request
from jose import JWTError, jwt

from src.config import get_settings

logger = logging.getLogger(__name__)


class Role(StrEnum):
    """User roles with hierarchical privilege levels (matches isnad-graph)."""

    VIEWER = "viewer"
    EDITOR = "editor"
    MODERATOR = "moderator"
    ADMIN = "admin"


ROLE_HIERARCHY: dict[Role, int] = {
    Role.VIEWER: 0,
    Role.EDITOR: 1,
    Role.MODERATOR: 2,
    Role.ADMIN: 3,
}

# user-service role string -> local Role. Matches isnad-graph's map so role
# resolution is identical across services.
_USER_SERVICE_ROLE_MAP: dict[str, Role] = {
    "admin": Role.ADMIN,
    "moderator": Role.MODERATOR,
    "researcher": Role.EDITOR,
    "editor": Role.EDITOR,
    "reader": Role.VIEWER,
    "viewer": Role.VIEWER,
    "trial": Role.VIEWER,
}


def _resolve_role(roles: list[str]) -> Role:
    """Pick the highest-privilege role from the JWT ``roles`` claim."""
    best = Role.VIEWER
    best_level = 0
    for r in roles:
        mapped = _USER_SERVICE_ROLE_MAP.get(r.lower(), Role.VIEWER)
        level = ROLE_HIERARCHY.get(mapped, 0)
        if level > best_level:
            best = mapped
            best_level = level
    return best


# ---------------------------------------------------------------------------
# JWKS-based RS256 JWT validation (mirrors isnad-graph/src/api/auth.py)
# ---------------------------------------------------------------------------

_jwks_cache: dict[str, object] | None = None
_jwks_fetched_at: float = 0.0


def _get_jwks_url() -> str:
    base = get_settings().auth.user_service_url.rstrip("/")
    return f"{base}/.well-known/jwks.json"


def fetch_jwks() -> dict[str, object]:
    """Fetch JWKS from user-service, using a TTL-based in-memory cache.

    Raises ``httpx.HTTPError`` if the JWKS endpoint is unreachable.
    """
    global _jwks_cache, _jwks_fetched_at  # noqa: PLW0603
    ttl = get_settings().auth.user_service_jwks_cache_ttl
    now = time.monotonic()
    if _jwks_cache is not None and (now - _jwks_fetched_at) < ttl:
        return _jwks_cache

    resp = httpx.get(_get_jwks_url(), timeout=10.0)
    resp.raise_for_status()
    _jwks_cache = resp.json()
    _jwks_fetched_at = now
    return _jwks_cache


def invalidate_jwks_cache() -> None:
    """Force the next ``fetch_jwks`` call to re-fetch from user-service."""
    global _jwks_cache, _jwks_fetched_at  # noqa: PLW0603
    _jwks_cache = None
    _jwks_fetched_at = 0.0


def verify_user_service_token(token: str) -> dict[str, object]:
    """Validate an RS256 JWT issued by user-service using its JWKS.

    Invalidates the cache and retries once on signature failure to handle
    key rotation. Returns the decoded payload.

    Raises:
        ValueError: token invalid, expired, or bad signature.
        httpx.HTTPError: JWKS endpoint unreachable.
    """
    jwks = fetch_jwks()
    try:
        payload: dict[str, object] = jwt.decode(token, jwks, algorithms=["RS256"])
    except JWTError:
        logger.info("JWKS verification failed, invalidating cache and retrying")
        invalidate_jwks_cache()
        jwks = fetch_jwks()
        try:
            payload = jwt.decode(token, jwks, algorithms=["RS256"])
        except JWTError as exc:
            raise ValueError(f"Invalid token: {exc}") from exc
    return payload


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------


def require_admin(request: Request) -> str:
    """FastAPI dependency: require an authenticated user-service admin.

    Returns the caller's identity (``sub``) so routes can attribute the
    reset in the audit log. Raises:

    - 401 — missing/invalid Authorization header, bad/expired token, or a
      non-``access`` token type.
    - 403 — valid token whose resolved role is below ADMIN.
    - 503 — user-service JWKS endpoint unreachable.
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")

    token = auth_header.removeprefix("Bearer ")

    try:
        payload = verify_user_service_token(token)
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid or expired token") from None
    except httpx.HTTPError:
        raise HTTPException(status_code=503, detail="Authentication service unavailable") from None

    if payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Invalid token type")

    subject = payload.get("sub")
    if not isinstance(subject, str):
        raise HTTPException(status_code=401, detail="Invalid token payload")

    roles_claim = payload.get("roles")
    roles_list: list[str] = roles_claim if isinstance(roles_claim, list) else []
    if _resolve_role(roles_list) != Role.ADMIN:
        raise HTTPException(status_code=403, detail="Admin access required")

    return subject
