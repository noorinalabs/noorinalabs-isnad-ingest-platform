"""Pipeline reset behaviour: stage, source, and full reset flows.

All tests use in-memory fakes — no AWS, Kafka, Neo4j, or Postgres
required. The core logic lives in ``src.pipeline.reset`` and is
deliberately free of hard infra deps so this suite is cheap to run.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from src.pipeline.reset import (
    ALL_PIPELINE_TOPICS,
    STAGE_PREFIXES,
    STAGE_TOPICS,
    PipelineResetter,
    ResetScope,
)


class _FakeS3Client:
    """In-memory S3 client supporting ``list_objects_v2`` + ``delete_objects``."""

    def __init__(self, objects: dict[tuple[str, str], bytes] | None = None) -> None:
        self.objects: dict[tuple[str, str], bytes] = dict(objects or {})

    def list_objects_v2(
        self, *, Bucket: str, Prefix: str, ContinuationToken: str | None = None
    ) -> dict[str, Any]:
        keys = sorted(k for (b, k) in self.objects if b == Bucket and k.startswith(Prefix))
        return {
            "Contents": [{"Key": k} for k in keys],
            "IsTruncated": False,
        }

    def delete_objects(self, *, Bucket: str, Delete: dict[str, Any]) -> dict[str, Any]:
        for obj in Delete["Objects"]:
            self.objects.pop((Bucket, obj["Key"]), None)
        return {"Deleted": Delete["Objects"]}


class _FakeStore:
    def __init__(self, bucket: str, client: _FakeS3Client) -> None:
        self.bucket = bucket
        self._client = client

    @property
    def client(self) -> _FakeS3Client:
        return self._client


class _FakeKafkaAdmin:
    def __init__(self) -> None:
        self.offset_resets: list[tuple[str, str]] = []
        self.topic_data_deletions: list[str] = []

    def reset_consumer_offsets(self, topic: str, group_id: str) -> None:
        self.offset_resets.append((topic, group_id))

    def delete_topic_data(self, topic: str) -> None:
        self.topic_data_deletions.append(topic)


class _FakeNeo4j:
    def __init__(self, rows: int = 0) -> None:
        self._rows = rows
        self.called = 0

    def truncate_hadith_graph(self) -> int:
        self.called += 1
        return self._rows


class _FakePg:
    def __init__(self, rows: int = 0) -> None:
        self._rows = rows
        self.called = 0

    def truncate_hadith_metadata(self) -> int:
        self.called += 1
        return self._rows


@pytest.fixture
def bucket_name() -> str:
    return "noorinalabs-pipeline"


@pytest.fixture
def seeded_store(bucket_name: str) -> _FakeStore:
    """Populate every stage prefix with two objects per source (2 sources)."""
    objects: dict[tuple[str, str], bytes] = {}
    for prefix in STAGE_PREFIXES.values():
        for source in ("sunnah-api", "thaqalayn"):
            objects[(bucket_name, f"{prefix}{source}/b1/file.parquet")] = b"x"
            objects[(bucket_name, f"{prefix}{source}/b2/file.parquet")] = b"x"
    return _FakeStore(bucket=bucket_name, client=_FakeS3Client(objects))


@pytest.fixture
def resetter(seeded_store: _FakeStore, tmp_path: Path) -> PipelineResetter:
    return PipelineResetter(
        object_store=seeded_store,
        kafka_admin=_FakeKafkaAdmin(),
        neo4j=_FakeNeo4j(rows=123),
        pg=_FakePg(rows=456),
        data_dir=tmp_path,
    )


def test_stage_reset_wipes_one_prefix_and_resets_consumer_offsets(
    seeded_store: _FakeStore, tmp_path: Path, bucket_name: str
) -> None:
    kafka_admin = _FakeKafkaAdmin()
    r = PipelineResetter(
        object_store=seeded_store,
        kafka_admin=kafka_admin,
        neo4j=_FakeNeo4j(),
        pg=_FakePg(),
        data_dir=tmp_path,
    )

    report, _entry, audit_path = r.reset(ResetScope.stage_scope("dedup"))

    assert report.level == "stage"
    assert report.s3_prefixes_deleted == ["dedup/"]
    assert report.s3_objects_deleted == 4  # 2 sources × 2 batches
    assert report.kafka_topics_reset == [STAGE_TOPICS["dedup"]]
    assert kafka_admin.offset_resets == [(STAGE_TOPICS["dedup"], "dedup-worker")]

    # Other prefixes untouched.
    remaining_prefixes = {k.split("/", 1)[0] + "/" for (_b, k) in seeded_store._client.objects}
    assert "dedup/" not in remaining_prefixes
    assert "raw/" in remaining_prefixes
    assert "enriched/" in remaining_prefixes

    assert audit_path.exists()


def test_source_reset_wipes_all_stages_for_one_source_only(
    seeded_store: _FakeStore, tmp_path: Path, bucket_name: str
) -> None:
    r = PipelineResetter(
        object_store=seeded_store,
        kafka_admin=_FakeKafkaAdmin(),
        neo4j=_FakeNeo4j(),
        pg=_FakePg(),
        data_dir=tmp_path,
    )

    report, _entry, _path = r.reset(ResetScope.source_scope("sunnah-api"))

    assert report.level == "source"
    # 5 stage prefixes × 2 batches = 10 objects for one source.
    assert report.s3_objects_deleted == 10

    remaining = seeded_store._client.objects
    assert all("sunnah-api" not in key for (_b, key) in remaining)
    # Other source untouched.
    assert any("thaqalayn" in key for (_b, key) in remaining)


def test_full_reset_wipes_everything_and_calls_neo4j_and_pg(
    seeded_store: _FakeStore, tmp_path: Path
) -> None:
    kafka_admin = _FakeKafkaAdmin()
    neo4j = _FakeNeo4j(rows=100)
    pg = _FakePg(rows=200)

    r = PipelineResetter(
        object_store=seeded_store,
        kafka_admin=kafka_admin,
        neo4j=neo4j,
        pg=pg,
        data_dir=tmp_path,
    )

    report, _entry, audit_path = r.reset(ResetScope.full_scope())

    assert report.level == "full"
    assert report.s3_objects_deleted == 20  # 5 prefixes × 2 sources × 2 batches
    assert set(report.kafka_topics_reset) == set(ALL_PIPELINE_TOPICS)
    assert kafka_admin.topic_data_deletions == list(ALL_PIPELINE_TOPICS)
    assert report.neo4j_rows_deleted == 100
    assert report.pg_rows_deleted == 200
    assert neo4j.called == 1
    assert pg.called == 1

    # Store is now empty.
    assert seeded_store._client.objects == {}
    # Audit entry was persisted.
    audit_data = json.loads(audit_path.read_text())
    assert audit_data["stage"] == "reset-full"
    assert audit_data["summary"]["level"] == "full"
    assert audit_data["summary"]["s3_objects_deleted"] == 20
    assert audit_data["rows_affected"] == 300  # neo4j + pg counted in audit


def test_audit_entry_is_written_per_reset(resetter: PipelineResetter, tmp_path: Path) -> None:
    resetter.reset(ResetScope.stage_scope("raw"))
    resetter.reset(ResetScope.stage_scope("dedup"))

    entries = sorted((tmp_path / "audit").glob("*.json"))
    assert len(entries) == 2

    stages = sorted(json.loads(p.read_text())["stage"] for p in entries)
    assert stages == ["reset-stage", "reset-stage"]


def test_stage_scope_rejects_unknown_stage() -> None:
    with pytest.raises(ValueError, match="unknown stage"):
        ResetScope.stage_scope("bogus")


def test_source_scope_rejects_empty_or_slashed() -> None:
    with pytest.raises(ValueError):
        ResetScope.source_scope("")
    with pytest.raises(ValueError):
        ResetScope.source_scope("evil/../path")


def test_full_reset_does_not_touch_user_tables_by_contract() -> None:
    """Guard test — the adapters module must never list user tables.

    Failure of this test would mean someone added a user table to the
    hadith truncate list and needs to revert that change.
    """
    from src.pipeline.reset_adapters import HADITH_METADATA_TABLES

    forbidden = {"users", "roles", "user_roles", "sessions", "subscriptions"}
    overlap = set(HADITH_METADATA_TABLES) & forbidden
    assert overlap == set(), f"user tables leaked into hadith truncate list: {overlap}"


def test_stage_reset_with_custom_consumer_group(seeded_store: _FakeStore, tmp_path: Path) -> None:
    kafka_admin = _FakeKafkaAdmin()
    r = PipelineResetter(
        object_store=seeded_store,
        kafka_admin=kafka_admin,
        neo4j=_FakeNeo4j(),
        pg=_FakePg(),
        data_dir=tmp_path,
    )

    r.reset(ResetScope.stage_scope("enriched", consumer_group="custom-group"))

    assert kafka_admin.offset_resets == [(STAGE_TOPICS["enriched"], "custom-group")]


def test_duration_is_recorded_in_report_and_audit(
    resetter: PipelineResetter, tmp_path: Path
) -> None:
    report, entry, path = resetter.reset(ResetScope.stage_scope("raw"))

    assert report.duration_seconds >= 0.0
    assert entry.duration_seconds == report.duration_seconds

    audit_data = json.loads(path.read_text())
    assert audit_data["duration_seconds"] == report.duration_seconds


def test_paginated_s3_deletion(bucket_name: str, tmp_path: Path) -> None:
    """Resetter must handle truncated list_objects_v2 responses."""

    class _PagingClient(_FakeS3Client):
        """Paginates the initial snapshot across two pages regardless of deletions
        that happen between the two list calls. Models real S3 list-then-delete."""

        def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
            super().__init__(objects)
            self._snapshot: list[str] | None = None

        def list_objects_v2(
            self, *, Bucket: str, Prefix: str, ContinuationToken: str | None = None
        ) -> dict[str, Any]:
            if ContinuationToken is None:
                self._snapshot = sorted(
                    k for (b, k) in self.objects if b == Bucket and k.startswith(Prefix)
                )
                return {
                    "Contents": [{"Key": k} for k in self._snapshot[:1]],
                    "IsTruncated": True,
                    "NextContinuationToken": "tok",
                }
            assert self._snapshot is not None
            return {
                "Contents": [{"Key": k} for k in self._snapshot[1:]],
                "IsTruncated": False,
            }

    objects = {(bucket_name, f"raw/sunnah-api/b1/{i}.parquet"): b"x" for i in range(3)}
    store = _FakeStore(bucket=bucket_name, client=_PagingClient(objects))
    r = PipelineResetter(
        object_store=store,
        kafka_admin=_FakeKafkaAdmin(),
        neo4j=_FakeNeo4j(),
        pg=_FakePg(),
        data_dir=tmp_path,
    )

    report, _entry, _path = r.reset(ResetScope.stage_scope("raw"))

    assert report.s3_objects_deleted == 3
    assert store._client.objects == {}
