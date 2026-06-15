"""Pipeline operational metrics — Kafka consumer lag + B2 object-store size.

Read-only metrics the isnad-graph admin data panel fetches over HTTP
(issue #76). Two independent reports:

- **Consumer lag** — per stage, per partition, how far each stage's
  consumer group is behind the head of the topic it consumes. The sum
  over partitions is the stage's backlog; the sum over stages is the
  pipeline backlog. This is the headline "is the pipeline keeping up?"
  signal for the admin panel.
- **Object-store size** — per B2 stage-prefix object count and byte
  total, plus the rollup. Tells operators how much staged data each
  pipeline stage is holding.

Both computations are pure functions over small injected Protocols
(``KafkaOffsetReader`` / ``ObjectSizeStore``) so the unit suite exercises
the lag arithmetic and the prefix-summing pagination against in-memory
fakes — no Kafka broker, no S3/B2. The live adapters live in
``src/pipeline/metrics_adapters.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from src.pipeline.stages import PIPELINE_STAGES, StageSpec

__all__ = [
    "KafkaOffsetReader",
    "LagReport",
    "ObjectSizeStore",
    "PartitionLag",
    "PrefixSize",
    "StageLag",
    "StorageReport",
    "compute_lag_report",
    "compute_storage_report",
]

# Canonical B2 stage prefixes whose size the storage report breaks down.
# Mirrors the object layout documented in issue #105 (and the reset surface's
# ``STAGE_PREFIXES``). Re-declared here rather than imported from
# ``src.pipeline.reset`` so the metrics surface does not depend on the reset
# module; ``tests/test_pipeline/test_metrics.py`` pins the two in sync.
STORAGE_PREFIXES: tuple[str, ...] = (
    "raw/",
    "dedup/",
    "enriched/",
    "normalized/",
    "staged/",
)


# ---------------------------------------------------------------------------
# Kafka consumer lag
# ---------------------------------------------------------------------------


class KafkaOffsetReader(Protocol):
    """Offset surface the lag computation needs.

    ``end_offsets`` returns the high-water mark (next offset to be written)
    per partition for a topic. ``committed_offsets`` returns the last
    committed offset per partition for a consumer group on a topic;
    partitions a group has never committed are simply absent from the map.
    """

    def end_offsets(self, topic: str) -> dict[int, int]: ...

    def committed_offsets(self, group_id: str, topic: str) -> dict[int, int]: ...


@dataclass(frozen=True)
class PartitionLag:
    """Lag for a single (topic, partition) under one consumer group."""

    partition: int
    end_offset: int
    committed_offset: int | None
    lag: int


@dataclass(frozen=True)
class StageLag:
    """Lag for one pipeline stage's consumer group on its consume topic."""

    stage: str
    consumer_group: str
    topic: str
    total_lag: int
    partitions: list[PartitionLag]


@dataclass(frozen=True)
class LagReport:
    """Consumer lag across every pipeline stage."""

    stages: list[StageLag]
    total_lag: int


def _stage_lag(reader: KafkaOffsetReader, stage: StageSpec) -> StageLag:
    end = reader.end_offsets(stage.consume_topic)
    committed = reader.committed_offsets(stage.consumer_group, stage.consume_topic)

    partitions: list[PartitionLag] = []
    total = 0
    for partition in sorted(end):
        end_offset = end[partition]
        committed_offset = committed.get(partition)
        # A partition the group has never committed is treated as fully
        # un-consumed (lag == head): the whole partition is backlog. Clamp
        # negative lag to 0 — a committed offset can momentarily exceed the
        # cached end offset under read skew, which is not a real backlog.
        baseline = committed_offset if committed_offset is not None else 0
        lag = max(0, end_offset - baseline)
        total += lag
        partitions.append(
            PartitionLag(
                partition=partition,
                end_offset=end_offset,
                committed_offset=committed_offset,
                lag=lag,
            )
        )

    return StageLag(
        stage=stage.name,
        consumer_group=stage.consumer_group,
        topic=stage.consume_topic,
        total_lag=total,
        partitions=partitions,
    )


def compute_lag_report(
    reader: KafkaOffsetReader,
    *,
    stages: tuple[StageSpec, ...] = PIPELINE_STAGES,
) -> LagReport:
    """Compute per-stage consumer lag and the pipeline-wide total."""
    stage_lags = [_stage_lag(reader, stage) for stage in stages]
    return LagReport(
        stages=stage_lags,
        total_lag=sum(s.total_lag for s in stage_lags),
    )


# ---------------------------------------------------------------------------
# B2 object-store size
# ---------------------------------------------------------------------------


class ObjectSizeStore(Protocol):
    """Subset of the S3/B2 client surface the size report needs.

    Matches ``workers.lib.object_store.ObjectStore`` (``bucket`` + a lazily
    constructed ``client``) so the same object the workers use satisfies
    this protocol.
    """

    bucket: str

    @property
    def client(self) -> Any: ...


@dataclass(frozen=True)
class PrefixSize:
    """Object count + byte total under one B2 prefix."""

    prefix: str
    object_count: int
    total_bytes: int


@dataclass(frozen=True)
class StorageReport:
    """Object-store size broken down by stage prefix, plus the rollup."""

    prefixes: list[PrefixSize]
    total_object_count: int
    total_bytes: int


def _sum_prefix(store: ObjectSizeStore, prefix: str) -> PrefixSize:
    """Sum object count + bytes under ``prefix`` via paginated listing.

    Uses the paginated ``list_objects_v2`` surface (same as the reset
    surface's ``_delete_prefix``) so a very large prefix is not truncated to
    a single response page.
    """
    client = store.client
    count = 0
    total_bytes = 0

    continuation: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": store.bucket, "Prefix": prefix}
        if continuation is not None:
            kwargs["ContinuationToken"] = continuation
        response = client.list_objects_v2(**kwargs)
        for obj in response.get("Contents", []) or []:
            count += 1
            total_bytes += int(obj.get("Size", 0))
        if not response.get("IsTruncated"):
            break
        continuation = response.get("NextContinuationToken")
        if continuation is None:
            break

    return PrefixSize(prefix=prefix, object_count=count, total_bytes=total_bytes)


def compute_storage_report(
    store: ObjectSizeStore,
    *,
    prefixes: tuple[str, ...] = STORAGE_PREFIXES,
) -> StorageReport:
    """Compute per-prefix object count + bytes and the bucket-wide rollup."""
    prefix_sizes = [_sum_prefix(store, prefix) for prefix in prefixes]
    return StorageReport(
        prefixes=prefix_sizes,
        total_object_count=sum(p.object_count for p in prefix_sizes),
        total_bytes=sum(p.total_bytes for p in prefix_sizes),
    )
