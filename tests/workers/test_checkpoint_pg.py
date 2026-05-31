"""Unit tests for PgCheckpoint with an in-memory psycopg-shaped fake.

The fake covers only the surface PgCheckpoint actually exercises:
``cursor()`` context manager yielding an object with ``execute`` and
``fetchone``, ``commit()``, and ``close()``. This keeps the unit suite
free of any real PostgreSQL dependency — the docker-backed restart
drill lives in ``tests/integration/test_checkpoint_pg_restart.py``.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from workers.lib.checkpoint_pg import PgCheckpoint


class _FakeCursor:
    def __init__(self, store: _FakePgConnection) -> None:
        self._store = store
        self._last_result: tuple[Any, ...] | None = None
        self.rowcount: int | None = None

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

        if normalized.startswith("SELECT COUNT(*) FROM PIPELINE.WORKER_CHECKPOINT"):
            assert params is not None
            (stage,) = params
            self._last_result = (sum(1 for r in self._store.rows if r[0] == stage),)
            return

        if normalized.startswith("INSERT INTO PIPELINE.WORKER_CHECKPOINT"):
            assert params is not None
            stage, batch_id = params
            self._store.rows.add((stage, batch_id))
            self._store.marked_at.setdefault((stage, batch_id), self._store.now)
            self._last_result = None
            return

        if normalized.startswith("DELETE FROM PIPELINE.WORKER_CHECKPOINT"):
            assert params is not None
            stage, retention_days = params
            cutoff = self._store.now - timedelta(days=int(retention_days))
            doomed = {
                r
                for r in self._store.rows
                if r[0] == stage and self._store.marked_at.get(r, self._store.now) < cutoff
            }
            self._store.rows -= doomed
            for r in doomed:
                self._store.marked_at.pop(r, None)
            self.rowcount = len(doomed)
            self._last_result = None
            return

        raise AssertionError(f"unexpected SQL in fake: {sql!r}")

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._last_result


class _FakePgConnection:
    """Psycopg-shaped fake: cursor()-context-manager + commit() + close().

    Tracks a per-row ``marked_at`` so the TTL sweep DELETE can be modelled
    against an injectable wall-clock (``now``).
    """

    def __init__(self) -> None:
        self.rows: set[tuple[str, str]] = set()
        self.marked_at: dict[tuple[str, str], datetime] = {}
        self.now = datetime.now(UTC)
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self.commits = 0
        self.closed = False

    def insert_aged(self, stage: str, batch_id: str, *, age_days: float) -> None:
        """Seed a row whose ``marked_at`` is ``age_days`` in the past."""
        self.rows.add((stage, batch_id))
        self.marked_at[(stage, batch_id)] = self.now - timedelta(days=age_days)

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


# --- TTL sweep (#42) ---------------------------------------------------------


def test_sweep_is_noop_below_threshold(conn: _FakePgConnection) -> None:
    """At or below the row-count threshold the sweep deletes nothing."""
    cp = PgCheckpoint(conn=conn, stage="dedup-worker", retention_days=14, threshold_rows=5)
    # Two rows older than the window, but well under the 5-row threshold.
    conn.insert_aged("dedup-worker", "batch-old-1", age_days=30)
    conn.insert_aged("dedup-worker", "batch-old-2", age_days=30)
    assert cp.sweep() == 0
    assert conn.rows == {("dedup-worker", "batch-old-1"), ("dedup-worker", "batch-old-2")}
    # No DELETE was issued.
    assert not any("DELETE" in sql.upper() for sql, _ in conn.executed)


def test_sweep_deletes_only_aged_rows_over_threshold(conn: _FakePgConnection) -> None:
    cp = PgCheckpoint(conn=conn, stage="dedup-worker", retention_days=14, threshold_rows=2)
    # Three rows trip the 2-row threshold; only the two older than 14d age out.
    conn.insert_aged("dedup-worker", "batch-stale-1", age_days=20)
    conn.insert_aged("dedup-worker", "batch-stale-2", age_days=15)
    conn.insert_aged("dedup-worker", "batch-fresh", age_days=3)
    deleted = cp.sweep()
    assert deleted == 2
    assert conn.rows == {("dedup-worker", "batch-fresh")}


def test_sweep_is_stage_scoped(conn: _FakePgConnection) -> None:
    """A sweep on one stage never touches another stage's rows."""
    dedup = PgCheckpoint(conn=conn, stage="dedup-worker", retention_days=14, threshold_rows=0)
    conn.insert_aged("dedup-worker", "batch-old", age_days=30)
    conn.insert_aged("enrich-worker", "batch-old", age_days=30)
    assert dedup.sweep() == 1
    assert ("enrich-worker", "batch-old") in conn.rows
    assert ("dedup-worker", "batch-old") not in conn.rows


def test_sweep_over_threshold_with_only_fresh_rows_deletes_nothing(
    conn: _FakePgConnection,
) -> None:
    """Over threshold but all rows inside the window — the alerting signal."""
    cp = PgCheckpoint(conn=conn, stage="dedup-worker", retention_days=14, threshold_rows=1)
    conn.insert_aged("dedup-worker", "batch-a", age_days=2)
    conn.insert_aged("dedup-worker", "batch-b", age_days=5)
    assert cp.sweep() == 0
    assert len(conn.rows) == 2


def test_retention_and_threshold_read_from_env(
    conn: _FakePgConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INGEST_CHECKPOINT_RETENTION_DAYS", "7")
    monkeypatch.setenv("INGEST_CHECKPOINT_SWEEP_THRESHOLD_ROWS", "1")
    cp = PgCheckpoint(conn=conn, stage="dedup-worker")
    conn.insert_aged("dedup-worker", "batch-stale", age_days=10)  # > 7d window
    conn.insert_aged("dedup-worker", "batch-fresh", age_days=4)  # < 7d window
    assert cp.sweep() == 1
    assert conn.rows == {("dedup-worker", "batch-fresh")}


def test_invalid_env_falls_back_to_default(
    conn: _FakePgConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INGEST_CHECKPOINT_RETENTION_DAYS", "not-an-int")
    # Falls back to the 14-day default rather than crashing construction.
    cp = PgCheckpoint(conn=conn, stage="dedup-worker", threshold_rows=0)
    conn.insert_aged("dedup-worker", "batch-stale", age_days=20)
    assert cp.sweep() == 1


def test_invalid_retention_argument_rejected(conn: _FakePgConnection) -> None:
    with pytest.raises(ValueError, match="retention_days must be positive"):
        PgCheckpoint(conn=conn, stage="dedup-worker", retention_days=0)


def test_invalid_threshold_argument_rejected(conn: _FakePgConnection) -> None:
    with pytest.raises(ValueError, match="threshold_rows must be non-negative"):
        PgCheckpoint(conn=conn, stage="dedup-worker", threshold_rows=-1)
