"""FastAPI app factory for the ingest-platform admin HTTP surface.

Exposes the admin-only reset endpoints (issue #70). The reset router is
mounted under ``/admin`` with ``Depends(require_admin)`` applied at the
router level, so every reset route is admin-only — there is no
unauthenticated path to a destructive operation.

Run locally with::

    uvicorn src.api.app:create_app --factory --port 8002
"""

from __future__ import annotations

from fastapi import Depends, FastAPI

from src.api.auth import require_admin
from src.api.reset_router import router as reset_router


def create_app() -> FastAPI:
    """Construct the FastAPI application."""
    app = FastAPI(
        title="isnad-ingest admin API",
        description="Admin-only pipeline operations (reset surface).",
        version="0.1.0",
    )

    @app.get("/healthz", tags=["health"])
    def healthz() -> dict[str, str]:
        """Unauthenticated liveness probe."""
        return {"status": "ok"}

    # Admin guard is applied once at the mount point — covers every reset route.
    app.include_router(
        reset_router,
        prefix="/admin",
        dependencies=[Depends(require_admin)],
    )

    return app


app = create_app()
