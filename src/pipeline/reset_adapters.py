"""Concrete Kafka / Neo4j / PG adapters used by :mod:`src.pipeline.reset`.

Split out from the resetter so the core logic has no hard dependency on
kafka-python or neo4j during unit tests. The CLI wires these adapters
in; tests substitute their own fakes.
"""

from __future__ import annotations

from typing import Any

__all__ = ["KafkaPythonAdmin", "Neo4jHadithResetter", "PgHadithMetadataResetter"]

# Tables that hold hadith metadata. User/auth tables are explicitly excluded.
# If new hadith tables are added, extend this list — NEVER add user tables.
HADITH_METADATA_TABLES: tuple[str, ...] = (
    "hadiths",
    "narrators",
    "narrator_mentions",
    "collections",
    "gradings",
    "parallel_links",
)

# Labels in Neo4j that hold hadith-graph data.
HADITH_NODE_LABELS: tuple[str, ...] = (
    "Hadith",
    "Narrator",
    "Chain",
    "Collection",
    "Grading",
    "HistoricalEvent",
    "Location",
)


class KafkaPythonAdmin:
    """kafka-python adapter for :class:`src.pipeline.reset.KafkaAdmin`.

    ``delete_topic_data`` does NOT delete the topic definition — just its
    records — by advancing all consumer groups past the high-water mark
    and relying on retention to clear storage. This preserves topic
    config (partitions, retention, compaction) across resets.
    """

    def __init__(self, admin_client: Any) -> None:
        self._admin = admin_client

    def reset_consumer_offsets(self, topic: str, group_id: str) -> None:
        # kafka-python exposes ``alter_consumer_group_offsets`` via the
        # underlying KafkaAdminClient. We set each partition to offset 0
        # for the given group, which re-runs consumption from the start.
        from kafka.admin.client import OffsetAndMetadata  # type: ignore[import-untyped]

        metadata = self._admin.describe_topics([topic])
        partitions = metadata[0]["partitions"] if metadata else []
        new_offsets = {(topic, p["partition"]): OffsetAndMetadata(0, "") for p in partitions}
        self._admin.alter_consumer_group_offsets(group_id, new_offsets)

    def delete_topic_data(self, topic: str) -> None:
        # Prefer ``delete_records`` (admin-only, retains topic config).
        self._admin.delete_records({topic: -1})


class Neo4jHadithResetter:
    """Neo4j driver adapter. Truncates only the hadith-graph labels."""

    def __init__(self, driver: Any) -> None:
        self._driver = driver

    def truncate_hadith_graph(self) -> int:
        total = 0
        with self._driver.session() as session:
            for label in HADITH_NODE_LABELS:
                # MATCH (n:Label) DETACH DELETE n — returns count so we
                # can aggregate into the audit entry.
                result = session.run(
                    f"MATCH (n:{label}) WITH n, 1 AS _ DETACH DELETE n RETURN count(_) AS c"
                )
                record = result.single()
                if record is not None:
                    total += int(record["c"])
        return total


class PgHadithMetadataResetter:
    """Postgres adapter. Truncates hadith metadata tables only —
    user/auth/RBAC tables are NEVER touched."""

    def __init__(self, connection: Any) -> None:
        self._conn = connection

    def truncate_hadith_metadata(self) -> int:
        total = 0
        with self._conn.cursor() as cur:
            for table in HADITH_METADATA_TABLES:
                # COUNT before TRUNCATE so the audit entry reflects what
                # was actually wiped. CASCADE handles FK constraints
                # within the hadith-metadata set — user tables have no
                # FK into these tables by design.
                cur.execute(f"SELECT count(*) FROM {table}")
                row = cur.fetchone()
                if row is not None:
                    total += int(row[0])
                cur.execute(f"TRUNCATE TABLE {table} CASCADE")
        self._conn.commit()
        return total
