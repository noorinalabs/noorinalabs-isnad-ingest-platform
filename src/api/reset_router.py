"""Admin-only HTTP endpoints wrapping the pipeline reset CLI surface.

Three endpoints mirror the three CLI reset scopes (issue #9):

- ``POST /admin/reset/stage``  — wipe one stage's B2 prefix + Kafka offsets
- ``POST /admin/reset/source`` — wipe every stage prefix for one source
- ``POST /admin/reset/full``   — full obliterate (B2 + Kafka + Neo4j + PG)

The destructive ``/full`` endpoint preserves the CLI's ``OBLITERATE``
confirmation semantics at the API boundary: the request MUST carry
``confirmation == "OBLITERATE"`` or the call is rejected before any work is
done. Every call routes through the real ``PipelineResetter``, which writes
an audit entry per reset (dry-run included).

The admin guard is applied at the router level in :mod:`src.api.app`
(``dependencies=[Depends(require_admin)]``), so every route here is
admin-only.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from src.api.deps import get_data_dir, get_resetter
from src.pipeline.reset import PipelineResetter, ResetScope, write_dry_run_audit

router = APIRouter(prefix="/reset", tags=["admin", "reset"])

# The literal an operator must supply to authorize a full reset. Mirrors the
# CLI's typed ``OBLITERATE`` confirmation (src/cli.py ``_cmd_reset``).
OBLITERATE_TOKEN = "OBLITERATE"


class _ResetRequestBase(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    dry_run: bool = Field(
        default=False,
        description="Validate scope and write a dry-run audit entry without touching infra.",
    )


class StageResetRequest(_ResetRequestBase):
    """Reset one pipeline stage."""

    stage: str = Field(description="Stage to reset: raw|dedup|enriched|normalized|staged")


class SourceResetRequest(_ResetRequestBase):
    """Reset every stage prefix for one data source."""

    source: str = Field(description="Source identifier, e.g. 'sunnah-api' (no '/').")


class FullResetRequest(_ResetRequestBase):
    """Full pipeline obliterate. Requires the OBLITERATE confirmation token."""

    confirmation: str = Field(
        description=f"Must equal '{OBLITERATE_TOKEN}' to authorize the destructive full reset.",
    )


class ResetResponse(BaseModel):
    """Result of a reset call — the resetter's report summary + audit path."""

    model_config = ConfigDict(frozen=True)

    level: str
    dry_run: bool
    confirmation_method: str
    audit_entry_path: str
    summary: dict[str, Any]


def _run_reset(
    scope: ResetScope,
    *,
    confirmation_method: str,
    dry_run: bool,
    resetter: PipelineResetter,
    data_dir: Path,
) -> ResetResponse:
    """Execute (or dry-run) a reset and shape the HTTP response.

    Both paths emit an audit entry — real resets via ``PipelineResetter.reset``,
    dry-runs via ``write_dry_run_audit`` — matching the CLI exactly.
    """
    if dry_run:
        report, _entry, audit_path = write_dry_run_audit(
            scope, confirmation_method=confirmation_method, data_dir=data_dir
        )
    else:
        report, _entry, audit_path = resetter.reset(scope, confirmation_method=confirmation_method)

    return ResetResponse(
        level=report.level,
        dry_run=report.dry_run,
        confirmation_method=report.confirmation_method,
        audit_entry_path=str(audit_path),
        summary=report.to_summary(),
    )


@router.post("/stage", response_model=ResetResponse)
def reset_stage(
    body: StageResetRequest,
    resetter: Annotated[PipelineResetter, Depends(get_resetter)],
    data_dir: Annotated[Path, Depends(get_data_dir)],
) -> ResetResponse:
    """Wipe one stage's B2 prefix and reset that stage's Kafka consumer offsets."""
    try:
        scope = ResetScope.stage_scope(body.stage)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # Stage scope is non-obliterate; the CLI records it as "not-required".
    return _run_reset(
        scope,
        confirmation_method="not-required",
        dry_run=body.dry_run,
        resetter=resetter,
        data_dir=data_dir,
    )


@router.post("/source", response_model=ResetResponse)
def reset_source(
    body: SourceResetRequest,
    resetter: Annotated[PipelineResetter, Depends(get_resetter)],
    data_dir: Annotated[Path, Depends(get_data_dir)],
) -> ResetResponse:
    """Wipe every stage prefix for one data source."""
    try:
        scope = ResetScope.source_scope(body.source)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return _run_reset(
        scope,
        confirmation_method="not-required",
        dry_run=body.dry_run,
        resetter=resetter,
        data_dir=data_dir,
    )


@router.post("/full", response_model=ResetResponse)
def reset_full(
    body: FullResetRequest,
    resetter: Annotated[PipelineResetter, Depends(get_resetter)],
    data_dir: Annotated[Path, Depends(get_data_dir)],
) -> ResetResponse:
    """Full obliterate. Rejects the call unless the OBLITERATE token is supplied.

    The token is the API-boundary equivalent of the operator typing
    ``OBLITERATE`` at the CLI prompt, so we record ``confirmation_method =
    "interactive"`` for SIEM parity with a typed confirmation.
    """
    if body.confirmation != OBLITERATE_TOKEN:
        raise HTTPException(
            status_code=400,
            detail=f"Full reset requires confirmation == '{OBLITERATE_TOKEN}'.",
        )

    return _run_reset(
        ResetScope.full_scope(),
        confirmation_method="interactive",
        dry_run=body.dry_run,
        resetter=resetter,
        data_dir=data_dir,
    )
