"""Canonical pipeline-stage topology (issue #76).

The worker pipeline runs four consuming stages in order::

    dedup -> enrich -> normalize -> graph-load

Each stage is a Kafka consumer group that reads one topic, processes a
batch, and (except the terminal ``graph-load`` stage) produces to the
next stage's topic. The metrics surface (consumer-lag) and the
reprocess-from-stage surface both need a single, authoritative table of
``(stage, consumer-group, consume-topic, produce-topic)`` so they read
and rewind exactly the topics/groups the workers actually use.

Why this lives in ``src`` rather than importing ``workers.lib.topics``
-----------------------------------------------------------------------
``src`` is the library layer; ``workers`` is the application layer built
on top of it (``workers`` imports ``src``, never the reverse). To keep
that direction clean — and to match the existing precedent in
``src/pipeline/reset.py``, which re-declares its stage→topic table rather
than importing from ``workers`` — the wire-level topic strings are
re-declared here as literals. ``tests/test_pipeline/test_stages.py`` pins
them against ``workers.lib.topics`` and the worker entrypoints' wiring, so
any drift between the two declarations breaks a test rather than silently
mis-reading a stage's lag.

The ``consumer_group`` string doubles as the **checkpoint stage key**: each
worker constructs its idempotency checkpoint with
``build_checkpoint(settings.worker_name)`` and ``worker_name ==
consumer_group`` for every stage, so the ``pipeline.worker_checkpoint``
rows for a stage are keyed by this same string. Reprocess relies on that
equivalence to clear the right checkpoint rows.
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["PIPELINE_STAGES", "STAGE_BY_NAME", "StageSpec", "downstream_stages"]


@dataclass(frozen=True)
class StageSpec:
    """One consuming pipeline stage.

    Attributes:
        name: Short stage identifier used by the admin UI and the
            reprocess API (``dedup`` | ``enrich`` | ``normalize`` |
            ``graph-load``).
        consumer_group: Kafka consumer-group id. Matches
            ``workers.<stage>.main``'s ``WorkerSettings.consumer_group`` and
            doubles as the ``pipeline.worker_checkpoint`` stage key.
        consume_topic: Topic this stage reads (where its lag is measured).
        produce_topic: Topic this stage writes, or ``None`` for the
            terminal ``graph-load`` stage.
    """

    name: str
    consumer_group: str
    consume_topic: str
    produce_topic: str | None


# Ordered dedup -> enrich -> normalize -> graph-load. The topic literals MUST
# equal the corresponding ``workers.lib.topics`` constants — pinned by
# tests/test_pipeline/test_stages.py.
PIPELINE_STAGES: tuple[StageSpec, ...] = (
    StageSpec(
        name="dedup",
        consumer_group="dedup-worker",
        consume_topic="pipeline.raw.landed",
        produce_topic="pipeline.dedup.done",
    ),
    StageSpec(
        name="enrich",
        consumer_group="enrich-worker",
        consume_topic="pipeline.dedup.done",
        produce_topic="pipeline.enrich.done",
    ),
    StageSpec(
        name="normalize",
        consumer_group="normalize-worker",
        consume_topic="pipeline.enrich.done",
        produce_topic="pipeline.normalize.done",
    ),
    StageSpec(
        name="graph-load",
        consumer_group="ingest-worker",
        consume_topic="pipeline.normalize.done",
        produce_topic=None,
    ),
)

STAGE_BY_NAME: dict[str, StageSpec] = {s.name: s for s in PIPELINE_STAGES}


def downstream_stages(stage_name: str) -> tuple[StageSpec, ...]:
    """Return ``stage_name`` and every stage after it, in pipeline order.

    Reprocessing from a stage re-delivers that stage's input and re-emits
    downstream, so the idempotency checkpoints for the named stage *and*
    every stage after it must be cleared for the re-delivered batches to be
    re-processed rather than skipped. Raises ``KeyError`` for an unknown
    stage name.
    """
    if stage_name not in STAGE_BY_NAME:
        raise KeyError(stage_name)
    names = [s.name for s in PIPELINE_STAGES]
    start = names.index(stage_name)
    return PIPELINE_STAGES[start:]
