"""Live Kafka adapter for the consumer-lag metric (issue #76).

Implements :class:`src.pipeline.metrics.KafkaOffsetReader` against a real
broker using ``kafka-python``:

- **end offsets** (high-water marks) come from a ``KafkaConsumer`` —
  ``partitions_for_topic`` + ``end_offsets`` — which is the only
  kafka-python surface that exposes the log-end offset.
- **committed offsets** for *another* group come from a
  ``KafkaAdminClient`` — ``list_consumer_group_offsets`` — so we can read a
  stage's committed position without joining its consumer group.

The object-store size metric needs no adapter here:
``workers.lib.object_store.ObjectStore`` already satisfies
:class:`src.pipeline.metrics.ObjectSizeStore` (``bucket`` + ``client``).

Both clients are injectable so the mapping logic is unit-tested with
fakes; in production they are built lazily from ``bootstrap_servers``.
"""

from __future__ import annotations

from typing import Any

__all__ = ["KafkaLagAdapter"]


class KafkaLagAdapter:
    """``KafkaOffsetReader`` backed by a live Kafka broker.

    ``admin_client`` and ``consumer`` are injected in tests; in production
    they are lazily constructed from ``bootstrap_servers`` on first use so
    importing this module never opens a broker connection.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str | None = None,
        admin_client: Any | None = None,
        consumer: Any | None = None,
    ) -> None:
        if admin_client is None and consumer is None and bootstrap_servers is None:
            raise ValueError("bootstrap_servers is required when clients are not injected")
        self._bootstrap = bootstrap_servers
        self._admin = admin_client
        self._consumer = consumer

    @property
    def admin(self) -> Any:
        if self._admin is None:
            from kafka.admin import KafkaAdminClient  # type: ignore[import-untyped]

            self._admin = KafkaAdminClient(bootstrap_servers=self._bootstrap)
        return self._admin

    @property
    def consumer(self) -> Any:
        if self._consumer is None:
            from kafka import KafkaConsumer  # type: ignore[import-untyped]

            # No group_id / no subscription: this consumer is only used for
            # metadata + end-offset lookups, never to consume records.
            self._consumer = KafkaConsumer(bootstrap_servers=self._bootstrap)
        return self._consumer

    def end_offsets(self, topic: str) -> dict[int, int]:
        # ``kafka`` is already flagged import-untyped at the KafkaConsumer
        # import above; a second import from the same module needs no ignore.
        from kafka import TopicPartition

        partitions = self.consumer.partitions_for_topic(topic)
        if not partitions:
            return {}
        tps = [TopicPartition(topic, p) for p in partitions]
        raw: dict[Any, int] = self.consumer.end_offsets(tps)
        return {tp.partition: int(offset) for tp, offset in raw.items()}

    def committed_offsets(self, group_id: str, topic: str) -> dict[int, int]:
        raw: dict[Any, Any] = self.admin.list_consumer_group_offsets(group_id)
        result: dict[int, int] = {}
        for tp, meta in raw.items():
            if tp.topic != topic:
                continue
            offset = int(meta.offset)
            # kafka-python uses -1 for "no committed offset"; omit so the lag
            # computation treats the partition as never-committed.
            if offset < 0:
                continue
            result[tp.partition] = offset
        return result
