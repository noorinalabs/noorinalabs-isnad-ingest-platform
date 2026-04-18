"""Pipeline reset operations — stage, source, and full reset.

Three reset scopes per issue #108:

- **stage**: wipe one B2 prefix + reset consumer offsets for that stage's
  Kafka topic. Use when a single stage produced bad output and needs
  re-running.
- **source**: wipe all B2 data for one source across every stage prefix.
  Use when re-acquiring a source from scratch.
- **full**: wipe every pipeline B2 prefix + every pipeline Kafka topic +
  Neo4j hadith graph + PG hadith metadata. **Preserves** users, roles,
  sessions, and Kafka broker/topic *definitions*.

Every reset emits an audit entry to ``data/audit/`` via the existing
``src/pipeline/audit.py`` module so operators have a record of who
cleared what and when.

All resource clients (``ObjectStore``, Kafka admin, Neo4j driver, PG
connection) are injected so the unit suite can exercise reset logic
against in-memory fakes without any infra.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from src.pipeline.audit import AuditEntry, create_audit_entry, write_audit_entry

__all__ = [
    "KafkaAdmin",
    "Neo4jResetter",
    "PgResetter",
    "PipelineResetter",
    "ResetReport",
    "ResetScope",
    "StageName",
    "S3Prefix",
]

# Canonical stage → B2 prefix mapping. Matches issue #105 layout.
STAGE_PREFIXES: dict[str, str] = {
    "raw": "raw/",
    "dedup": "dedup/",
    "enriched": "enriched/",
    "normalized": "normalized/",
    "staged": "staged/",
}

# Canonical stage → Kafka topic mapping. Matches issue #106 contract.
STAGE_TOPICS: dict[str, str] = {
    "raw": "pipeline.raw.new",
    "dedup": "pipeline.dedup.done",
    "enriched": "pipeline.enrich.done",
    "normalized": "pipeline.norm.done",
}

# All pipeline topics — used by full reset.
ALL_PIPELINE_TOPICS: tuple[str, ...] = (
    "pipeline.raw.new",
    "pipeline.dedup.done",
    "pipeline.enrich.done",
    "pipeline.norm.done",
    "pipeline.dlq",
)

StageName = str
S3Prefix = str


class _ObjectStoreProto(Protocol):
    """Subset of the S3 client surface the resetter needs.

    Matches ``workers/lib/object_store.py``'s ``ObjectStore`` client so
    the same object works in both code paths.
    """

    bucket: str

    @property
    def client(self) -> Any: ...


class KafkaAdmin(Protocol):
    """Kafka admin surface. Wraps ``kafka-python``'s ``KafkaAdminClient``.

    Only two methods needed — everything else we do is consumer-group
    offset reset via the built-in kafka-consumer-groups CLI or programmatic
    equivalent.
    """

    def reset_consumer_offsets(self, topic: str, group_id: str) -> None: ...
    def delete_topic_data(self, topic: str) -> None: ...


class Neo4jResetter(Protocol):
    """Neo4j surface. ``truncate_hadith_graph`` deletes every node except
    user-related labels (currently none live in Neo4j, but the contract
    is future-proofed)."""

    def truncate_hadith_graph(self) -> int: ...


class PgResetter(Protocol):
    """PostgreSQL surface. Truncates hadith metadata tables only —
    never touches ``users``, ``roles``, ``user_roles``, ``sessions``."""

    def truncate_hadith_metadata(self) -> int: ...


@dataclass(frozen=True)
class ResetScope:
    """What to reset. Exactly one of the three factory classmethods is used."""

    level: str  # "stage" | "source" | "full"
    stage: str | None = None  # set when level == "stage"
    source: str | None = None  # set when level == "source"
    consumer_group: str | None = None  # defaults to stage name for stage reset

    @classmethod
    def stage_scope(cls, stage: str, *, consumer_group: str | None = None) -> ResetScope:
        if stage not in STAGE_PREFIXES:
            raise ValueError(f"unknown stage: {stage!r}. Known: {sorted(STAGE_PREFIXES)}")
        return cls(level="stage", stage=stage, consumer_group=consumer_group or f"{stage}-worker")

    @classmethod
    def source_scope(cls, source: str) -> ResetScope:
        if not source or "/" in source:
            raise ValueError(f"invalid source identifier: {source!r}")
        return cls(level="source", source=source)

    @classmethod
    def full_scope(cls) -> ResetScope:
        return cls(level="full")


@dataclass
class ResetReport:
    """What a reset actually did. Saved into the audit log summary."""

    level: str
    s3_prefixes_deleted: list[str] = field(default_factory=list)
    s3_objects_deleted: int = 0
    kafka_topics_reset: list[str] = field(default_factory=list)
    neo4j_rows_deleted: int = 0
    pg_rows_deleted: int = 0
    duration_seconds: float = 0.0

    def to_summary(self) -> dict[str, Any]:
        return {
            "level": self.level,
            "s3_prefixes_deleted": self.s3_prefixes_deleted,
            "s3_objects_deleted": self.s3_objects_deleted,
            "kafka_topics_reset": self.kafka_topics_reset,
            "neo4j_rows_deleted": self.neo4j_rows_deleted,
            "pg_rows_deleted": self.pg_rows_deleted,
        }


class PipelineResetter:
    """Coordinates B2 / Kafka / Neo4j / PG resets and writes audit entries.

    Dependencies are injected so tests can exercise full reset flows
    against fakes — no AWS, Kafka, Neo4j, or Postgres required.
    """

    def __init__(
        self,
        *,
        object_store: _ObjectStoreProto,
        kafka_admin: KafkaAdmin,
        neo4j: Neo4jResetter,
        pg: PgResetter,
        data_dir: Path,
    ) -> None:
        self.object_store = object_store
        self.kafka_admin = kafka_admin
        self.neo4j = neo4j
        self.pg = pg
        self.data_dir = data_dir

    def reset(self, scope: ResetScope) -> tuple[ResetReport, AuditEntry, Path]:
        """Execute a reset and persist an audit entry.

        Returns the report, the audit entry, and the path where the
        audit entry was written.
        """
        start = time.monotonic()

        if scope.level == "stage":
            report = self._reset_stage(scope)
        elif scope.level == "source":
            report = self._reset_source(scope)
        elif scope.level == "full":
            report = self._reset_full()
        else:
            raise ValueError(f"unknown reset level: {scope.level!r}")

        report.duration_seconds = round(time.monotonic() - start, 3)

        entry = create_audit_entry(
            stage=f"reset-{scope.level}",
            duration_seconds=report.duration_seconds,
            rows_affected=report.neo4j_rows_deleted + report.pg_rows_deleted,
            summary=report.to_summary(),
        )
        path = write_audit_entry(self.data_dir, entry)
        return report, entry, path

    def _reset_stage(self, scope: ResetScope) -> ResetReport:
        """Wipe one B2 prefix + reset that stage's consumer group offsets."""
        assert scope.stage is not None
        prefix = STAGE_PREFIXES[scope.stage]
        topic = STAGE_TOPICS.get(scope.stage)

        deleted = _delete_prefix(self.object_store, prefix)

        topics_reset: list[str] = []
        if topic is not None and scope.consumer_group is not None:
            self.kafka_admin.reset_consumer_offsets(topic, scope.consumer_group)
            topics_reset.append(topic)

        return ResetReport(
            level="stage",
            s3_prefixes_deleted=[prefix],
            s3_objects_deleted=deleted,
            kafka_topics_reset=topics_reset,
        )

    def _reset_source(self, scope: ResetScope) -> ResetReport:
        """Wipe every stage prefix for one source."""
        assert scope.source is not None
        source_prefixes = [f"{p}{scope.source}/" for p in STAGE_PREFIXES.values()]

        total_deleted = 0
        for sp in source_prefixes:
            total_deleted += _delete_prefix(self.object_store, sp)

        return ResetReport(
            level="source",
            s3_prefixes_deleted=source_prefixes,
            s3_objects_deleted=total_deleted,
        )

    def _reset_full(self) -> ResetReport:
        """Wipe every pipeline prefix, reset every pipeline topic, truncate
        Neo4j hadith graph and PG hadith metadata. Preserves user/auth data."""
        prefixes = list(STAGE_PREFIXES.values())
        total_deleted = 0
        for prefix in prefixes:
            total_deleted += _delete_prefix(self.object_store, prefix)

        topics_reset: list[str] = []
        for topic in ALL_PIPELINE_TOPICS:
            self.kafka_admin.delete_topic_data(topic)
            topics_reset.append(topic)

        neo4j_rows = self.neo4j.truncate_hadith_graph()
        pg_rows = self.pg.truncate_hadith_metadata()

        return ResetReport(
            level="full",
            s3_prefixes_deleted=prefixes,
            s3_objects_deleted=total_deleted,
            kafka_topics_reset=topics_reset,
            neo4j_rows_deleted=neo4j_rows,
            pg_rows_deleted=pg_rows,
        )


def _delete_prefix(store: _ObjectStoreProto, prefix: S3Prefix) -> int:
    """Delete every object under ``prefix`` in ``store.bucket``.

    Returns the number of objects deleted. Uses the paginated ``list_objects_v2``
    surface so very large prefixes don't blow past a single-response limit.
    """
    client = store.client
    deleted = 0

    continuation: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": store.bucket, "Prefix": prefix}
        if continuation is not None:
            kwargs["ContinuationToken"] = continuation
        response = client.list_objects_v2(**kwargs)
        contents = response.get("Contents", []) or []
        if contents:
            to_delete = [{"Key": obj["Key"]} for obj in contents]
            client.delete_objects(Bucket=store.bucket, Delete={"Objects": to_delete, "Quiet": True})
            deleted += len(to_delete)
        if not response.get("IsTruncated"):
            break
        continuation = response.get("NextContinuationToken")
        if continuation is None:
            break

    return deleted
