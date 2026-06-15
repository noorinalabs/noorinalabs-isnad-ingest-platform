"""FastAPI app factory for the ingest-platform admin HTTP surface.

Exposes the admin-only pipeline operations the isnad-graph admin data panel
drives: the reset endpoints (issue #70), the read-only metrics endpoints
(consumer lag + object-store size) and the reprocess-from-stage endpoint
(issue #76). Every router is mounted under ``/admin`` with
``Depends(require_admin)`` applied at the mount point, so there is no
unauthenticated path to any operation — destructive or read-only.

Run locally with::

    uvicorn src.api.app:create_app --factory --port 8002
"""

from __future__ import annotations

from fastapi import Depends, FastAPI

from src.api.auth import require_admin
from src.api.metrics_router import router as metrics_router
from src.api.reprocess_router import router as reprocess_router
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

    # Admin guard is applied once per mount point — covers every admin route
    # (reset, metrics, reprocess) with no unauthenticated path to any of them.
    admin_guard = [Depends(require_admin)]
    app.include_router(reset_router, prefix="/admin", dependencies=admin_guard)
    app.include_router(metrics_router, prefix="/admin", dependencies=admin_guard)
    app.include_router(reprocess_router, prefix="/admin", dependencies=admin_guard)

    return app


app = create_app()
