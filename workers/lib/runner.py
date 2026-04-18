"""Worker consumer loop.

Workers implement a single ``process(msg) -> PipelineMessage | None``
function. :class:`WorkerRunner` wraps it with:

- Kafka consume/commit
- DLQ routing on any uncaught exception
- Idempotency guard (skip if ``batch_id`` already processed)
- Metrics emission

The Kafka client (``kafka-python``) and object store are injected at
construction so the tests can run with in-memory fakes — no Kafka, no
S3, no Docker required for the unit suite.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from workers.lib.dlq import DLQ_TOPIC, build_dlq_record
from workers.lib.log import get_logger
from workers.lib.message import PipelineMessage, parse_message, serialize_message
from workers.lib.metrics import Metrics, time_batch

__all__ = ["ProcessFn", "WorkerRunner", "WorkerSettings"]

_logger = get_logger("workers.runner")


class ProcessFn(Protocol):
    """Per-batch processor. Returns the next-stage message, or ``None`` to drop."""

    def __call__(self, msg: PipelineMessage) -> PipelineMessage | None: ...


class WorkerSettings:
    """Injection point for Kafka topics and consumer-group identity."""

    def __init__(
        self,
        *,
        worker_name: str,
        consume_topic: str,
        produce_topic: str | None,
        consumer_group: str,
        dlq_topic: str = DLQ_TOPIC,
    ) -> None:
        self.worker_name = worker_name
        self.consume_topic = consume_topic
        self.produce_topic = produce_topic
        self.consumer_group = consumer_group
        self.dlq_topic = dlq_topic


class _Checkpoint(Protocol):
    """Minimal idempotency store."""

    def seen(self, batch_id: str) -> bool: ...
    def mark(self, batch_id: str) -> None: ...


class InMemoryCheckpoint:
    """Default idempotency store. Loses state on restart — fine for dev.

    TODO: swap for a durable checkpoint store (Postgres table or
    S3-backed marker objects) before production rollout.
    """

    def __init__(self) -> None:
        self._seen: set[str] = set()

    def seen(self, batch_id: str) -> bool:
        return batch_id in self._seen

    def mark(self, batch_id: str) -> None:
        self._seen.add(batch_id)


class WorkerRunner:
    """Wraps a ``process`` callable into a resilient Kafka consumer loop."""

    def __init__(
        self,
        *,
        settings: WorkerSettings,
        consumer: Any,
        producer: Any,
        process: ProcessFn,
        checkpoint: _Checkpoint | None = None,
        metrics: Metrics | None = None,
    ) -> None:
        self.settings = settings
        self.consumer = consumer
        self.producer = producer
        self.process = process
        self.checkpoint = checkpoint or InMemoryCheckpoint()
        self.metrics = metrics or Metrics(settings.worker_name)

    def handle_one(self, raw_value: bytes | str | dict[str, Any]) -> None:
        """Process a single Kafka message value. Testable without a real consumer."""
        msg = parse_message(raw_value)

        if self.checkpoint.seen(msg.batch_id):
            _logger.info(
                "idempotent_skip",
                worker=self.settings.worker_name,
                batch_id=msg.batch_id,
            )
            return

        try:
            with time_batch(self.metrics, batch_id=msg.batch_id):
                next_msg = self.process(msg)
        except Exception as exc:  # noqa: BLE001 — DLQ catches all failures
            record = build_dlq_record(worker=self.settings.worker_name, original=msg, exc=exc)
            self.producer.send(self.settings.dlq_topic, record.to_bytes())
            self.metrics.errors(batch_id=msg.batch_id, error_class=type(exc).__name__)
            _logger.error(
                "worker_failed",
                worker=self.settings.worker_name,
                batch_id=msg.batch_id,
                error_class=type(exc).__name__,
                error=str(exc),
            )
            return

        self.metrics.records_processed(msg.record_count, batch_id=msg.batch_id)
        self.checkpoint.mark(msg.batch_id)

        if next_msg is not None and self.settings.produce_topic is not None:
            self.producer.send(self.settings.produce_topic, serialize_message(next_msg))

    def run_forever(self, *, stop: Callable[[], bool] | None = None) -> None:
        """Consume indefinitely. ``stop`` is a hook the tests use to break the loop."""
        for record in self.consumer:
            self.handle_one(record.value)
            if stop is not None and stop():
                break
