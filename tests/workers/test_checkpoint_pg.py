"""Unit tests for PgCheckpoint with an in-memory psycopg-shaped fake.

The fake covers only the surface PgCheckpoint actually exercises:
``cursor()`` context manager yielding an object with ``execute`` and
``fetchone``, ``commit()``, and ``close()``. This keeps the unit suite
free of any real PostgreSQL dependency — the docker-backed restart
drill lives in ``tests/integration/test_checkpoint_pg_restart.py``.
"""

from __future__ import annotations

import re
from typing import Any

import pytest

from workers.lib.checkpoint_pg import PgCheckpoint


class _FakeCursor:
    def __init__(self, store: _FakePgConnection) -> None:
        self._store = store
        self._last_result: tuple[int] | None = None

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        self._store.executed.append((sql, params))
        normalized = re.sub(r"\s+", " ", sql).strip().upper()

        if (
            normalized.startswith("CREATE SCHEMA")
            or "CREATE TABLE" in normalized
            or "CREATE INDEX" in normalized
        ):
            self._last_result = None
            return

        if normalized.startswith("SELECT 1 FROM PIPELINE.WORKER_CHECKPOINT"):
            assert params is not None
            stage, batch_id = params
            self._last_result = (1,) if (stage, batch_id) in self._store.rows else None
            return

        if normalized.startswith("INSERT INTO PIPELINE.WORKER_CHECKPOINT"):
            assert params is not None
            stage, batch_id = params
            self._store.rows.add((stage, batch_id))
            self._last_result = None
            return

        raise AssertionError(f"unexpected SQL in fake: {sql!r}")

    def fetchone(self) -> tuple[int] | None:
        return self._last_result


class _FakePgConnection:
    """Psycopg-shaped fake: cursor()-context-manager + commit() + close()."""

    def __init__(self) -> None:
        self.rows: set[tuple[str, str]] = set()
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self.commits = 0
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def conn() -> _FakePgConnection:
    return _FakePgConnection()


def test_ddl_runs_at_construction(conn: _FakePgConnection) -> None:
    PgCheckpoint(conn=conn, stage="dedup-worker")
    # The DDL is one execute call; commit happens once during construction.
    assert conn.commits == 1
    assert any("CREATE SCHEMA" in sql.upper() for sql, _ in conn.executed)
    assert any("CREATE TABLE" in sql.upper() for sql, _ in conn.executed)
    assert any("CREATE INDEX" in sql.upper() for sql, _ in conn.executed)


def test_empty_stage_rejected(conn: _FakePgConnection) -> None:
    with pytest.raises(ValueError, match="non-empty"):
        PgCheckpoint(conn=conn, stage="")


def test_mark_then_seen_roundtrip(conn: _FakePgConnection) -> None:
    cp = PgCheckpoint(conn=conn, stage="dedup-worker")
    assert cp.seen("batch-001") is False
    cp.mark("batch-001")
    assert cp.seen("batch-001") is True


def test_mark_is_idempotent_via_on_conflict(conn: _FakePgConnection) -> None:
    cp = PgCheckpoint(conn=conn, stage="dedup-worker")
    cp.mark("batch-001")
    cp.mark("batch-001")  # second mark is a no-op via ON CONFLICT DO NOTHING
    assert cp.seen("batch-001") is True
    # Exactly one row landed.
    assert sum(1 for r in conn.rows if r[1] == "batch-001") == 1


def test_stage_scopes_seen_lookups(conn: _FakePgConnection) -> None:
    """Same batch_id flowing through two stages gets two distinct rows."""
    dedup = PgCheckpoint(conn=conn, stage="dedup-worker")
    enrich = PgCheckpoint(conn=conn, stage="enrich-worker")
    dedup.mark("batch-001")
    assert dedup.seen("batch-001") is True
    assert enrich.seen("batch-001") is False  # different stage, not yet marked
    enrich.mark("batch-001")
    assert enrich.seen("batch-001") is True


def test_close_propagates_to_underlying_connection(conn: _FakePgConnection) -> None:
    cp = PgCheckpoint(conn=conn, stage="dedup-worker")
    cp.close()
    assert conn.closed is True
