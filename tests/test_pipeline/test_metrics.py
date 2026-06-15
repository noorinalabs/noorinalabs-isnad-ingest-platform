"""Pipeline metrics: consumer-lag arithmetic + object-store size summing (#76).

All core tests run against in-memory fakes — no Kafka broker, no S3/B2. The
live Kafka adapter's offset/partition mapping is exercised with fake admin +
consumer objects keyed by real ``kafka.TopicPartition`` namedtuples.
"""

from __future__ import annotations

from collections import namedtuple
from typing import Any

from src.pipeline.metrics import (
    STORAGE_PREFIXES,
    compute_lag_report,
    compute_storage_report,
)
from src.pipeline.stages import PIPELINE_STAGES

# ---------------------------------------------------------------------------
# Consumer lag
# ---------------------------------------------------------------------------


class _FakeOffsetReader:
    """Returns canned end/committed offsets keyed by topic / (group, topic)."""

    def __init__(
        self,
        *,
        end: dict[str, dict[int, int]],
        committed: dict[tuple[str, str], dict[int, int]],
    ) -> None:
        self._end = end
        self._committed = committed
        self.end_calls: list[str] = []
        self.committed_calls: list[tuple[str, str]] = []

    def end_offsets(self, topic: str) -> dict[int, int]:
        self.end_calls.append(topic)
        return self._end.get(topic, {})

    def committed_offsets(self, group_id: str, topic: str) -> dict[int, int]:
        self.committed_calls.append((group_id, topic))
        return self._committed.get((group_id, topic), {})


def _all_topics_reader(end_each: int, committed_each: int) -> _FakeOffsetReader:
    """Reader giving every stage one partition with the given end/committed."""
    end = {s.consume_topic: {0: end_each} for s in PIPELINE_STAGES}
    committed = {(s.consumer_group, s.consume_topic): {0: committed_each} for s in PIPELINE_STAGES}
    return _FakeOffsetReader(end=end, committed=committed)


def test_lag_report_covers_every_stage_with_per_partition_lag() -> None:
    reader = _all_topics_reader(end_each=100, committed_each=90)
    report = compute_lag_report(reader)

    assert [s.stage for s in report.stages] == ["dedup", "enrich", "normalize", "graph-load"]
    for stage in report.stages:
        assert stage.total_lag == 10
        assert len(stage.partitions) == 1
        assert stage.partitions[0].lag == 10
        assert stage.partitions[0].end_offset == 100
        assert stage.partitions[0].committed_offset == 90
    # 4 stages * 10 lag each.
    assert report.total_lag == 40


def test_lag_sums_across_multiple_partitions() -> None:
    dedup = PIPELINE_STAGES[0]
    reader = _FakeOffsetReader(
        end={dedup.consume_topic: {0: 50, 1: 30, 2: 10}},
        committed={(dedup.consumer_group, dedup.consume_topic): {0: 40, 1: 30, 2: 0}},
    )
    report = compute_lag_report(reader, stages=(dedup,))
    stage = report.stages[0]
    # partition lags: 10, 0, 10 -> total 20; partitions sorted by id.
    assert [p.partition for p in stage.partitions] == [0, 1, 2]
    assert [p.lag for p in stage.partitions] == [10, 0, 10]
    assert stage.total_lag == 20
    assert report.total_lag == 20


def test_uncommitted_partition_treated_as_full_backlog() -> None:
    dedup = PIPELINE_STAGES[0]
    reader = _FakeOffsetReader(
        end={dedup.consume_topic: {0: 75}},
        committed={},  # group never committed
    )
    stage = compute_lag_report(reader, stages=(dedup,)).stages[0]
    assert stage.partitions[0].committed_offset is None
    assert stage.partitions[0].lag == 75
    assert stage.total_lag == 75


def test_committed_ahead_of_end_clamps_to_zero() -> None:
    dedup = PIPELINE_STAGES[0]
    reader = _FakeOffsetReader(
        end={dedup.consume_topic: {0: 10}},
        committed={(dedup.consumer_group, dedup.consume_topic): {0: 12}},
    )
    stage = compute_lag_report(reader, stages=(dedup,)).stages[0]
    assert stage.partitions[0].lag == 0


def test_empty_topic_yields_zero_lag_no_partitions() -> None:
    dedup = PIPELINE_STAGES[0]
    reader = _FakeOffsetReader(end={}, committed={})
    stage = compute_lag_report(reader, stages=(dedup,)).stages[0]
    assert stage.partitions == []
    assert stage.total_lag == 0


# ---------------------------------------------------------------------------
# Object-store size
# ---------------------------------------------------------------------------


class _FakeSizeS3Client:
    """In-memory S3 client supporting paginated ``list_objects_v2`` with sizes."""

    def __init__(self, objects: dict[tuple[str, str], int], page_size: int = 1000) -> None:
        # objects: (bucket, key) -> size in bytes
        self.objects = dict(objects)
        self._page_size = page_size

    def list_objects_v2(
        self, *, Bucket: str, Prefix: str, ContinuationToken: str | None = None
    ) -> dict[str, Any]:
        keys = sorted(k for (b, k) in self.objects if b == Bucket and k.startswith(Prefix))
        start = int(ContinuationToken) if ContinuationToken is not None else 0
        page = keys[start : start + self._page_size]
        contents = [{"Key": k, "Size": self.objects[(Bucket, k)]} for k in page]
        next_start = start + self._page_size
        truncated = next_start < len(keys)
        out: dict[str, Any] = {"Contents": contents, "IsTruncated": truncated}
        if truncated:
            out["NextContinuationToken"] = str(next_start)
        return out


class _FakeStore:
    def __init__(self, bucket: str, client: _FakeSizeS3Client) -> None:
        self.bucket = bucket
        self._client = client

    @property
    def client(self) -> _FakeSizeS3Client:
        return self._client


def test_storage_prefixes_match_reset_layout() -> None:
    """The storage breakdown uses the same B2 stage prefixes the reset surface does."""
    from src.pipeline.reset import STAGE_PREFIXES

    assert set(STORAGE_PREFIXES) == set(STAGE_PREFIXES.values())


def test_storage_report_sums_count_and_bytes_per_prefix() -> None:
    bucket = "noorinalabs-pipeline"
    objects = {
        (bucket, "raw/sunnah/a.parquet"): 100,
        (bucket, "raw/sunnah/b.parquet"): 200,
        (bucket, "dedup/sunnah/a.parquet"): 50,
        (bucket, "staged/x.parquet"): 7,
        # an object outside the known prefixes is ignored by the per-prefix sum
        (bucket, "other/z.parquet"): 9999,
    }
    store = _FakeStore(bucket, _FakeSizeS3Client(objects))
    report = compute_storage_report(store)

    by_prefix = {p.prefix: p for p in report.prefixes}
    assert by_prefix["raw/"].object_count == 2
    assert by_prefix["raw/"].total_bytes == 300
    assert by_prefix["dedup/"].object_count == 1
    assert by_prefix["dedup/"].total_bytes == 50
    assert by_prefix["staged/"].total_bytes == 7
    assert by_prefix["enriched/"].object_count == 0
    # rollup excludes the out-of-prefix object.
    assert report.total_object_count == 4
    assert report.total_bytes == 357


def test_storage_report_paginates_large_prefix() -> None:
    bucket = "noorinalabs-pipeline"
    objects = {(bucket, f"raw/{i:04d}.parquet"): 10 for i in range(2500)}
    store = _FakeStore(bucket, _FakeSizeS3Client(objects, page_size=1000))
    report = compute_storage_report(store, prefixes=("raw/",))
    assert report.prefixes[0].object_count == 2500
    assert report.prefixes[0].total_bytes == 25000


# ---------------------------------------------------------------------------
# Live Kafka adapter mapping (no broker — fake admin/consumer)
# ---------------------------------------------------------------------------

_Meta = namedtuple("_Meta", "offset")


class _FakeConsumer:
    def __init__(self, partitions: dict[str, set[int]], end: dict[Any, int]) -> None:
        self._partitions = partitions
        self._end = end

    def partitions_for_topic(self, topic: str) -> set[int] | None:
        return self._partitions.get(topic)

    def end_offsets(self, tps: list[Any]) -> dict[Any, int]:
        return {tp: self._end[tp] for tp in tps}


class _FakeAdmin:
    def __init__(self, offsets: dict[str, dict[Any, _Meta]]) -> None:
        self._offsets = offsets

    def list_consumer_group_offsets(self, group_id: str) -> dict[Any, _Meta]:
        return self._offsets.get(group_id, {})


def test_kafka_lag_adapter_maps_end_and_committed_offsets() -> None:
    from kafka import TopicPartition  # type: ignore[import-untyped]

    from src.pipeline.metrics_adapters import KafkaLagAdapter

    topic = "pipeline.raw.landed"
    tp0, tp1 = TopicPartition(topic, 0), TopicPartition(topic, 1)
    other = TopicPartition("other.topic", 0)

    consumer = _FakeConsumer(partitions={topic: {0, 1}}, end={tp0: 100, tp1: 200})
    admin = _FakeAdmin(
        offsets={
            "dedup-worker": {
                tp0: _Meta(90),
                tp1: _Meta(-1),  # no committed offset -> omitted
                other: _Meta(5),  # different topic -> filtered out
            }
        }
    )
    adapter = KafkaLagAdapter(admin_client=admin, consumer=consumer)

    assert adapter.end_offsets(topic) == {0: 100, 1: 200}
    assert adapter.committed_offsets("dedup-worker", topic) == {0: 90}
    assert adapter.end_offsets("missing.topic") == {}


def test_kafka_lag_adapter_requires_bootstrap_or_clients() -> None:
    from src.pipeline.metrics_adapters import KafkaLagAdapter

    try:
        KafkaLagAdapter()
    except ValueError:
        pass
    else:  # pragma: no cover - guard
        raise AssertionError("expected ValueError without bootstrap or injected clients")
