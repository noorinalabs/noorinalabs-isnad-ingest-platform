"""Admin-only HTTP endpoint to reprocess the pipeline from a chosen stage (#76).

``POST /admin/reprocess`` rewinds one stage's consumer group and clears the
idempotency checkpoints for that stage and everything downstream, re-running
the pipeline from there **without deleting staged B2 data** (that is the
reset surface's job — :mod:`src.api.reset_router`).

The endpoint mirrors the reset router's shape: a ``dry_run`` flag returns the
resolved plan and writes a dry-run audit entry without touching infra, and
the live reprocessor is built (via ``get_reprocessor_factory``) ONLY in the
non-dry-run branch so a preview never opens Kafka/Postgres connections. The
admin guard is applied at the mount point in :mod:`src.api.app`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from src.api.deps import get_data_dir, get_reprocessor_factory
from src.pipeline.reprocess import PipelineReprocessor, plan_reprocess, write_dry_run_audit

router = APIRouter(prefix="/reprocess", tags=["admin", "reprocess"])


class ReprocessRequest(BaseModel):
    """Reprocess the pipeline from ``stage`` onward."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    stage: str = Field(
        description="Stage to reprocess from: dedup|enrich|normalize|graph-load",
    )
    dry_run: bool = Field(
        default=False,
        description="Resolve the plan and write a dry-run audit entry without touching infra.",
    )


class ReprocessResponse(BaseModel):
    """Result of a reprocess call — the report summary + audit path."""

    model_config = ConfigDict(frozen=True)

    from_stage: str
    dry_run: bool
    audit_entry_path: str
    summary: dict[str, Any]


@router.post("", response_model=ReprocessResponse)
def reprocess(
    body: ReprocessRequest,
    reprocessor_factory: Annotated[
        Callable[[], PipelineReprocessor], Depends(get_reprocessor_factory)
    ],
    data_dir: Annotated[Path, Depends(get_data_dir)],
) -> ReprocessResponse:
    """Reprocess from ``body.stage``. Unknown stage -> 422; dry-run is inert."""
    # Validate the stage up front so a bad name is rejected (422) before any
    # client is built — for both the dry-run and the live path.
    try:
        plan_reprocess(body.stage)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    if body.dry_run:
        report, _entry, audit_path = write_dry_run_audit(body.stage, data_dir=data_dir)
    else:
        reprocessor = reprocessor_factory()
        report, _entry, audit_path = reprocessor.reprocess(body.stage)

    return ReprocessResponse(
        from_stage=report.from_stage,
        dry_run=report.dry_run,
        audit_entry_path=str(audit_path),
        summary=report.to_summary(),
    )
