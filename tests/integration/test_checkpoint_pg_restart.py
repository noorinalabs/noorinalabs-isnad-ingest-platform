"""Restart-survival integration test for PgCheckpoint.

Spins up a real PostgreSQL container via testcontainers, marks a batch,
constructs a *fresh* PgCheckpoint against the same DSN (simulating a
worker process restart), and asserts the mark survives.

This is the W11 acceptance gate the issue cares about: in-memory state
is lost on restart, PG state is not.
"""

from __future__ import annotations

import pytest

psycopg = pytest.importorskip("psycopg")

from workers.lib.checkpoint_pg import PgCheckpoint  # noqa: E402

pytestmark = pytest.mark.integration


def _connect(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn)


def test_checkpoint_survives_worker_restart(postgres_container) -> None:
    dsn = postgres_container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")

    # Worker process 1 — mark a batch, then exit (close connection).
    conn1 = _connect(dsn)
    cp1 = PgCheckpoint(conn=conn1, stage="dedup-worker")
    assert cp1.seen("batch-restart-001") is False
    cp1.mark("batch-restart-001")
    assert cp1.seen("batch-restart-001") is True
    cp1.close()

    # Worker process 2 — fresh PgCheckpoint, fresh connection, same DSN.
    # The DDL is idempotent so a second construction is harmless.
    conn2 = _connect(dsn)
    cp2 = PgCheckpoint(conn=conn2, stage="dedup-worker")
    assert cp2.seen("batch-restart-001") is True, (
        "checkpoint was lost across restart — durability regression"
    )
    # Another stage on the same DSN sees its own scope.
    cp2_enrich = PgCheckpoint(conn=conn2, stage="enrich-worker")
    assert cp2_enrich.seen("batch-restart-001") is False
    cp2.close()


def test_schema_creation_is_idempotent(postgres_container) -> None:
    """Two back-to-back constructions on the same DSN must not error."""
    dsn = postgres_container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
    conn = _connect(dsn)
    PgCheckpoint(conn=conn, stage="dedup-worker")
    # Second construction re-runs the DDL — must be a no-op, not an error.
    PgCheckpoint(conn=conn, stage="dedup-worker")
    conn.close()
