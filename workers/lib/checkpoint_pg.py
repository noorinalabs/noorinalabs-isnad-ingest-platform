"""Durable PostgreSQL-backed worker checkpoint.

Implements the ``_Checkpoint`` protocol in :mod:`workers.lib.runner`,
backing the idempotency guard with a row per ``(stage, batch_id)`` in
``pipeline.worker_checkpoint``.

The schema is created idempotently at construction time, mirroring the
no-Alembic pattern used by ``src.utils.pg_client.PgClient.ensure_schema``.
"""

from __future__ import annotations

from typing import Any, Protocol

__all__ = ["PgCheckpoint", "_PgConnection"]


class _PgConnection(Protocol):
    """Minimal subset of ``psycopg.Connection`` we depend on.

    Lets the unit tests pass a fake without importing psycopg.
    """

    def cursor(self) -> Any: ...
    def commit(self) -> None: ...
    def close(self) -> None: ...


_DDL = """
CREATE SCHEMA IF NOT EXISTS pipeline;

CREATE TABLE IF NOT EXISTS pipeline.worker_checkpoint (
    stage      text        NOT NULL,
    batch_id   text        NOT NULL,
    marked_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (stage, batch_id)
);

CREATE INDEX IF NOT EXISTS worker_checkpoint_marked_at_idx
    ON pipeline.worker_checkpoint (marked_at);
"""


class PgCheckpoint:
    """Per-stage durable idempotency store.

    Each ``(stage, batch_id)`` pair is one row. ``seen`` is a single
    indexed lookup; ``mark`` is ``INSERT ... ON CONFLICT DO NOTHING``
    so a redelivery after a partial failure is a no-op.

    A TTL sweep is intentionally out of scope — at the expected rate
    (~10k batches/day × 7-day Kafka retention ≈ 70k rows) the index
    stays trivially small. See follow-up issue (filed post-#11) for
    the explicit TTL job when scale warrants it.
    """

    def __init__(self, *, conn: _PgConnection, stage: str) -> None:
        if not stage:
            raise ValueError("stage must be a non-empty string")
        self._conn = conn
        self._stage = stage
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(_DDL)
        self._conn.commit()

    def seen(self, batch_id: str) -> bool:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pipeline.worker_checkpoint WHERE stage = %s AND batch_id = %s",
                (self._stage, batch_id),
            )
            return cur.fetchone() is not None

    def mark(self, batch_id: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pipeline.worker_checkpoint (stage, batch_id) "
                "VALUES (%s, %s) ON CONFLICT (stage, batch_id) DO NOTHING",
                (self._stage, batch_id),
            )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
