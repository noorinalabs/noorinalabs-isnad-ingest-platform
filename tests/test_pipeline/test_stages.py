"""Pin the canonical pipeline-stage table (issue #76).

``src.pipeline.stages`` re-declares the wire-level topic strings and
consumer-group ids rather than importing ``workers.lib.topics`` (``src``
stays independent of ``workers``). These tests are the coupling that makes
any drift between the two declarations — or between the table and the worker
entrypoints' actual wiring — fail loudly rather than silently mis-reading a
stage's lag or rewinding the wrong group.
"""

from __future__ import annotations

from src.pipeline.stages import (
    PIPELINE_STAGES,
    STAGE_BY_NAME,
    StageSpec,
    downstream_stages,
)
from workers.lib import topics


def test_stage_topics_match_workers_topic_constants() -> None:
    """Each stage's consume/produce topic equals the workers' SSOT constant."""
    expected: dict[str, tuple[str, str | None]] = {
        "dedup": (topics.PIPELINE_RAW_LANDED, topics.PIPELINE_DEDUP_DONE),
        "enrich": (topics.PIPELINE_DEDUP_DONE, topics.PIPELINE_ENRICH_DONE),
        "normalize": (topics.PIPELINE_ENRICH_DONE, topics.PIPELINE_NORMALIZE_DONE),
        "graph-load": (topics.PIPELINE_NORMALIZE_DONE, None),
    }
    for stage in PIPELINE_STAGES:
        assert (stage.consume_topic, stage.produce_topic) == expected[stage.name]


def test_consumer_groups_match_worker_entrypoints() -> None:
    """Consumer-group ids match ``workers.<stage>.main``'s WorkerSettings.

    These strings double as the ``pipeline.worker_checkpoint`` stage keys,
    so a rename here must be a conscious cross-module decision.
    """
    groups = {s.name: s.consumer_group for s in PIPELINE_STAGES}
    assert groups == {
        "dedup": "dedup-worker",
        "enrich": "enrich-worker",
        "normalize": "normalize-worker",
        "graph-load": "ingest-worker",
    }


def test_stage_chain_is_contiguous() -> None:
    """Every non-terminal stage produces exactly the next stage's consume topic."""
    for prev, nxt in zip(PIPELINE_STAGES, PIPELINE_STAGES[1:], strict=False):
        assert prev.produce_topic == nxt.consume_topic
    assert PIPELINE_STAGES[-1].produce_topic is None  # graph-load is terminal


def test_stage_by_name_covers_every_stage() -> None:
    assert set(STAGE_BY_NAME) == {s.name for s in PIPELINE_STAGES}
    for name, spec in STAGE_BY_NAME.items():
        assert isinstance(spec, StageSpec)
        assert spec.name == name


def test_downstream_stages_returns_stage_and_successors_in_order() -> None:
    assert [s.name for s in downstream_stages("dedup")] == [
        "dedup",
        "enrich",
        "normalize",
        "graph-load",
    ]
    assert [s.name for s in downstream_stages("normalize")] == ["normalize", "graph-load"]
    assert [s.name for s in downstream_stages("graph-load")] == ["graph-load"]


def test_downstream_stages_unknown_raises() -> None:
    try:
        downstream_stages("bogus")
    except KeyError:
        pass
    else:  # pragma: no cover - guard
        raise AssertionError("expected KeyError for unknown stage")
