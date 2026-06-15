"""Durable PostgreSQL-backed worker checkpoint.

Implements the ``_Checkpoint`` protocol in :mod:`workers.lib.runner`,
backing the idempotency guard with a row per ``(stage, batch_id)`` in
``pipeline.worker_checkpoint``.

The schema is created idempotently at construction time, mirroring the
no-Alembic pattern used by ``src.utils.pg_client.PgClient.ensure_schema``.
"""

from __future__ import annotations

import os
from typing import Any, Protocol

from workers.lib.log import get_logger

__all__ = ["CHECKPOINT_TABLE", "PgCheckpoint", "_PgConnection"]

_logger = get_logger("workers.checkpoint_pg")

# Fully-qualified checkpoint table. Exported so out-of-worker callers that must
# clear checkpoint rows by stage — the reprocess surface
# (``src/pipeline/reprocess_adapters.py``) — reference the same table this
# module's DDL creates instead of re-deriving the name.
CHECKPOINT_TABLE = "pipeline.worker_checkpoint"

# TTL sweep defaults (#42). The sweep is a no-op until the row count for a
# stage exceeds ``_SWEEP_THRESHOLD_ROWS``; below that the index stays trivially
# small and a periodic DELETE is pure operational cost. Once over threshold,
# rows older than ``_SWEEP_RETENTION_DAYS`` are deleted. The retention window
# defaults to Kafka's 7-day retention plus a one-week safety margin, so a row
# is only ever swept well after its batch could possibly be redelivered.
#
# Both are read at construction time from the environment so ops can tune them
# without a code change (see RUNBOOK § "worker_checkpoint TTL sweep").
_DEFAULT_RETENTION_DAYS = 14
_DEFAULT_THRESHOLD_ROWS = 100_000


class _PgConnection(Protocol):
    """Minimal subset of ``psycopg.Connection`` we depend on.

    Lets the unit tests pass a fake without importing psycopg.
    """

    def cursor(self) -> Any: ...
    def commit(self) -> None: ...
    def close(self) -> None: ...


_DDL = f"""
CREATE SCHEMA IF NOT EXISTS pipeline;

CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
    stage      text        NOT NULL,
    batch_id   text        NOT NULL,
    marked_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (stage, batch_id)
);

CREATE INDEX IF NOT EXISTS worker_checkpoint_marked_at_idx
    ON {CHECKPOINT_TABLE} (marked_at);
"""


def _env_int(name: str, default: int) -> int:
    """Read ``name`` from the environment as an int, falling back to ``default``.

    A blank or unparseable value falls back to the default rather than
    crashing a worker on a typo'd env var; the chosen value is observable
    in the ``checkpoint_sweep`` metric.
    """
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        _logger.warning("checkpoint_env_int_invalid", name=name, value=raw, default=default)
        return default


class PgCheckpoint:
    """Per-stage durable idempotency store.

    Each ``(stage, batch_id)`` pair is one row. ``seen`` is a single
    indexed lookup; ``mark`` is ``INSERT ... ON CONFLICT DO NOTHING``
    so a redelivery after a partial failure is a no-op.

    At the expected rate (~10k batches/day × 7-day Kafka retention ≈
    70k rows) the ``marked_at`` index stays trivially small, so the
    TTL :meth:`sweep` is a no-op until a stage's row count crosses
    ``threshold_rows``. A throughput rise, an extended Kafka retention,
    or a backfill/replay spike trips the sweep, which then deletes rows
    older than ``retention_days``. See #42.
    """

    def __init__(
        self,
        *,
        conn: _PgConnection,
        stage: str,
        retention_days: int | None = None,
        threshold_rows: int | None = None,
    ) -> None:
        if not stage:
            raise ValueError("stage must be a non-empty string")
        self._conn = conn
        self._stage = stage
        self._retention_days = (
            retention_days
            if retention_days is not None
            else _env_int("INGEST_CHECKPOINT_RETENTION_DAYS", _DEFAULT_RETENTION_DAYS)
        )
        self._threshold_rows = (
            threshold_rows
            if threshold_rows is not None
            else _env_int("INGEST_CHECKPOINT_SWEEP_THRESHOLD_ROWS", _DEFAULT_THRESHOLD_ROWS)
        )
        if self._retention_days <= 0:
            raise ValueError("retention_days must be positive")
        if self._threshold_rows < 0:
            raise ValueError("threshold_rows must be non-negative")
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

    def _row_count(self) -> int:
        """Number of checkpoint rows for this stage."""
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pipeline.worker_checkpoint WHERE stage = %s",
                (self._stage,),
            )
            row = cur.fetchone()
        return int(row[0]) if row is not None else 0

    def sweep(self) -> int:
        """Delete aged-out checkpoint rows once the stage exceeds its threshold.

        No-op (returns ``0``) while the stage's row count is at or below
        ``threshold_rows`` — below that the index stays trivially small and a
        DELETE is pure operational cost. Once over threshold, deletes rows whose
        ``marked_at`` is older than ``retention_days`` and returns the number
        deleted.

        Emits a ``checkpoint_sweep`` metric (rows deleted + pre-sweep row count +
        the active thresholds) so an external scheduler can alert on a sweep that
        deletes nothing despite a high row count — the signal that retention is
        mis-tuned for the current throughput.
        """
        row_count = self._row_count()
        if row_count <= self._threshold_rows:
            _logger.info(
                "checkpoint_sweep",
                stage=self._stage,
                deleted=0,
                row_count=row_count,
                threshold_rows=self._threshold_rows,
                retention_days=self._retention_days,
                swept=False,
            )
            return 0

        with self._conn.cursor() as cur:
            # make_interval keeps the retention window a bound parameter rather
            # than string-interpolating into an INTERVAL literal.
            cur.execute(
                "DELETE FROM pipeline.worker_checkpoint "
                "WHERE stage = %s AND marked_at < now() - make_interval(days => %s)",
                (self._stage, self._retention_days),
            )
            deleted = int(cur.rowcount) if cur.rowcount is not None else 0
        self._conn.commit()

        _logger.info(
            "checkpoint_sweep",
            stage=self._stage,
            deleted=deleted,
            row_count=row_count,
            threshold_rows=self._threshold_rows,
            retention_days=self._retention_days,
            swept=True,
        )
        return deleted

    def close(self) -> None:
        self._conn.close()
