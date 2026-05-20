"""Env-driven checkpoint backend selection for worker entrypoints.

Reads ``INGEST_CHECKPOINT_BACKEND`` (``in-memory`` | ``pg``) and
constructs the matching ``_Checkpoint`` implementation. ``b2`` is
reserved for a future B2 marker-object backend; selecting it today
raises ``NotImplementedError`` so callers can express the eventual
shape without us silently degrading to in-memory.

Production workers default to ``pg`` via the Dockerfile ENV; local
``make dev`` keeps the in-memory default so no PG is required to
exercise the consumer loop.
"""

from __future__ import annotations

import os

from workers.lib.runner import InMemoryCheckpoint, _Checkpoint

__all__ = ["build_checkpoint"]

_VALID = ("in-memory", "pg", "b2")


def build_checkpoint(stage: str) -> _Checkpoint:
    """Return a checkpoint for ``stage`` driven by ``INGEST_CHECKPOINT_BACKEND``."""
    backend = os.environ.get("INGEST_CHECKPOINT_BACKEND", "in-memory").lower()

    if backend == "in-memory":
        return InMemoryCheckpoint()

    if backend == "pg":
        # Lazy import so unit-test workers don't require psycopg installed.
        import psycopg

        from src.config import get_settings
        from workers.lib.checkpoint_pg import PgCheckpoint

        dsn = get_settings().postgres.dsn
        conn = psycopg.connect(dsn)
        return PgCheckpoint(conn=conn, stage=stage)

    if backend == "b2":
        raise NotImplementedError(
            "INGEST_CHECKPOINT_BACKEND=b2 is reserved for a future "
            "B2 marker-object backend; use 'in-memory' or 'pg' today."
        )

    raise ValueError(f"INGEST_CHECKPOINT_BACKEND={backend!r} is invalid; expected one of {_VALID}.")
