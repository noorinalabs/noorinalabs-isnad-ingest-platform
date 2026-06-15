"""Concrete checkpoint adapter for :mod:`src.pipeline.reprocess` (issue #76).

The Kafka side of a reprocess reuses
``src.pipeline.reset_adapters.KafkaPythonAdmin`` (its
``reset_consumer_offsets`` rewinds a group to offset 0), so the only new
adapter here is the checkpoint resetter that clears
``pipeline.worker_checkpoint`` rows for a stage.

``_CHECKPOINT_TABLE`` is re-declared rather than imported from
``workers.lib.checkpoint_pg`` to keep ``src`` independent of ``workers``
(``workers`` imports ``src``, never the reverse — see
``src/pipeline/stages.py``). ``tests/test_pipeline/test_reprocess.py`` pins
it against ``workers.lib.checkpoint_pg.CHECKPOINT_TABLE`` so the two
declarations cannot drift.
"""

from __future__ import annotations

from typing import Any, Protocol

__all__ = ["PgCheckpointResetter"]

# Must equal workers.lib.checkpoint_pg.CHECKPOINT_TABLE (pinned by test).
_CHECKPOINT_TABLE = "pipeline.worker_checkpoint"


class _PgConnection(Protocol):
    """Minimal psycopg connection surface the resetter needs.

    Mirrors ``workers.lib.checkpoint_pg._PgConnection`` so a fake (or a real
    ``psycopg.Connection``) works without importing psycopg here.
    """

    def cursor(self) -> Any: ...
    def commit(self) -> None: ...
    def close(self) -> None: ...


class PgCheckpointResetter:
    """Clears ``pipeline.worker_checkpoint`` rows for a stage.

    Implements ``src.pipeline.reprocess.CheckpointResetter``. One connection
    serves every stage in a reprocess — the resetter is constructed once and
    ``clear`` is called per affected stage.
    """

    def __init__(self, conn: _PgConnection) -> None:
        self._conn = conn

    def clear(self, stage_key: str) -> int:
        """Delete every checkpoint row for ``stage_key``; return the row count.

        The table name is a fixed module constant (never request-derived), so
        interpolating it into the statement carries no injection risk; the
        stage key is a bound parameter.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {_CHECKPOINT_TABLE} WHERE stage = %s",
                (stage_key,),
            )
            deleted = int(cur.rowcount) if cur.rowcount is not None else 0
        self._conn.commit()
        return deleted

    def close(self) -> None:
        self._conn.close()
