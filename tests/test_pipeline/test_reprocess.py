"""Reprocess-from-stage behaviour (issue #76).

All tests use in-memory fakes — no Kafka, no Postgres. Covers the
orchestration (clear downstream checkpoints, rewind the entry stage's
group), the dry-run inert path, audit emission, the unknown-stage guard,
and the PG checkpoint-resetter SQL.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from src.pipeline.reprocess import (
    PipelineReprocessor,
    plan_reprocess,
    write_dry_run_audit,
)


class _RecordingKafka:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.resets: list[tuple[str, str]] = []

    def reset_consumer_offsets(self, topic: str, group_id: str) -> None:
        self.resets.append((topic, group_id))
        self.events.append(f"rewind:{group_id}")


class _RecordingCheckpoints:
    def __init__(self, events: list[str], counts: dict[str, int] | None = None) -> None:
        self.events = events
        self._counts = counts or {}
        self.cleared: list[str] = []

    def clear(self, stage_key: str) -> int:
        self.cleared.append(stage_key)
        self.events.append(f"clear:{stage_key}")
        return self._counts.get(stage_key, 0)


def _read_audit(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def test_reprocess_clears_downstream_checkpoints_and_rewinds_entry_group(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    kafka = _RecordingKafka(events)
    checkpoints = _RecordingCheckpoints(
        events,
        counts={"enrich-worker": 3, "normalize-worker": 5, "ingest-worker": 2},
    )
    r = PipelineReprocessor(kafka=kafka, checkpoints=checkpoints, data_dir=tmp_path)

    report, _entry, path = r.reprocess("enrich")

    # Entry stage + downstream checkpoints cleared (by consumer-group key).
    assert checkpoints.cleared == ["enrich-worker", "normalize-worker", "ingest-worker"]
    # Only the entry stage's group is rewound, on its consume topic.
    assert kafka.resets == [("pipeline.dedup.done", "enrich-worker")]

    assert report.from_stage == "enrich"
    assert report.rewound_group == "enrich-worker"
    assert report.rewound_topic == "pipeline.dedup.done"
    assert report.affected_stages == ["enrich", "normalize", "graph-load"]
    assert report.checkpoints_cleared == {
        "enrich": 3,
        "normalize": 5,
        "graph-load": 2,
    }
    assert report.dry_run is False

    audit = _read_audit(path)
    assert audit["stage"] == "reprocess-enrich"
    assert audit["rows_affected"] == 10  # 3 + 5 + 2
    assert audit["summary"]["from_stage"] == "enrich"


def test_reprocess_clears_checkpoints_before_rewinding(tmp_path: Path) -> None:
    """A re-delivered batch must never be skippable: clear precedes rewind."""
    events: list[str] = []
    kafka = _RecordingKafka(events)
    checkpoints = _RecordingCheckpoints(events)
    r = PipelineReprocessor(kafka=kafka, checkpoints=checkpoints, data_dir=tmp_path)

    r.reprocess("normalize")

    assert events == ["clear:normalize-worker", "clear:ingest-worker", "rewind:normalize-worker"]


def test_reprocess_from_terminal_stage_only_touches_itself(tmp_path: Path) -> None:
    events: list[str] = []
    kafka = _RecordingKafka(events)
    checkpoints = _RecordingCheckpoints(events)
    r = PipelineReprocessor(kafka=kafka, checkpoints=checkpoints, data_dir=tmp_path)

    report, _entry, _path = r.reprocess("graph-load")
    assert checkpoints.cleared == ["ingest-worker"]
    assert kafka.resets == [("pipeline.normalize.done", "ingest-worker")]
    assert report.affected_stages == ["graph-load"]


def test_reprocess_unknown_stage_raises(tmp_path: Path) -> None:
    r = PipelineReprocessor(
        kafka=_RecordingKafka([]), checkpoints=_RecordingCheckpoints([]), data_dir=tmp_path
    )
    with pytest.raises(ValueError, match="unknown stage"):
        r.reprocess("bogus")


def test_plan_reprocess_resolves_without_executing() -> None:
    plan = plan_reprocess("dedup")
    assert plan.from_stage == "dedup"
    assert plan.rewound_group == "dedup-worker"
    assert plan.rewound_topic == "pipeline.raw.landed"
    assert plan.affected_stages == ["dedup", "enrich", "normalize", "graph-load"]
    assert "REPROCESS from stage 'dedup'" in plan.describe()


def test_plan_reprocess_unknown_stage_raises() -> None:
    with pytest.raises(ValueError, match="unknown stage"):
        plan_reprocess("bogus")


def test_dry_run_audit_writes_entry_without_work(tmp_path: Path) -> None:
    report, _entry, path = write_dry_run_audit("dedup", data_dir=tmp_path)
    assert report.dry_run is True
    assert report.checkpoints_cleared == {
        "dedup": 0,
        "enrich": 0,
        "normalize": 0,
        "graph-load": 0,
    }
    audit = _read_audit(path)
    assert audit["stage"] == "reprocess-dedup"
    assert audit["rows_affected"] == 0
    assert audit["summary"]["dry_run"] is True
    # The entry landed under <data_dir>/audit/.
    assert path.parent == tmp_path / "audit"


# ---------------------------------------------------------------------------
# PG checkpoint resetter
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, rowcount: int) -> None:
        self.rowcount = rowcount
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[Any, ...]) -> None:
        self.executed.append((sql, params))


class _FakeConn:
    def __init__(self, rowcount: int) -> None:
        self._cursor = _FakeCursor(rowcount)
        self.commits = 0
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        self.closed = True


def test_pg_checkpoint_resetter_deletes_by_stage_and_commits() -> None:
    from src.pipeline.reprocess_adapters import PgCheckpointResetter

    conn = _FakeConn(rowcount=7)
    resetter = PgCheckpointResetter(conn)
    deleted = resetter.clear("enrich-worker")

    assert deleted == 7
    assert conn.commits == 1
    sql, params = conn._cursor.executed[0]
    assert "DELETE FROM" in sql
    assert "WHERE stage = %s" in sql
    assert params == ("enrich-worker",)


def test_pg_checkpoint_resetter_table_matches_worker_ddl() -> None:
    """The resetter targets the SAME table the workers' checkpoint DDL creates."""
    from src.pipeline import reprocess_adapters
    from workers.lib.checkpoint_pg import CHECKPOINT_TABLE

    assert reprocess_adapters._CHECKPOINT_TABLE == CHECKPOINT_TABLE


def test_pg_checkpoint_resetter_null_rowcount_is_zero() -> None:
    from src.pipeline.reprocess_adapters import PgCheckpointResetter

    conn = _FakeConn(rowcount=None)  # type: ignore[arg-type]
    assert PgCheckpointResetter(conn).clear("dedup-worker") == 0
