"""WorkerRunner behaviour: success, DLQ routing, idempotency."""

from __future__ import annotations

import json

import pytest

from tests.workers.conftest import FakeConsumer, FakeProducer
from tests.workers.test_checkpoint_pg import _FakePgConnection
from workers.lib.checkpoint_pg import PgCheckpoint
from workers.lib.dlq import DLQ_TOPIC
from workers.lib.message import PipelineMessage, parse_message, serialize_message
from workers.lib.runner import InMemoryCheckpoint, WorkerRunner, WorkerSettings


class _SendFailsProducer(FakeProducer):
    """Producer whose downstream ``send`` raises, simulating broker
    unavailability / queue-full in the mark/send window (#43).

    DLQ sends still succeed so the failure is isolated to the
    next-stage publish path.
    """

    def __init__(self, *, failing_topic: str) -> None:
        super().__init__()
        self._failing_topic = failing_topic

    def send(self, topic: str, value: bytes) -> None:
        if topic == self._failing_topic:
            raise RuntimeError("broker unavailable")
        super().send(topic, value)


def _settings() -> WorkerSettings:
    return WorkerSettings(
        worker_name="test-worker",
        consume_topic="in",
        produce_topic="out",
        consumer_group="g",
    )


def test_handle_one_forwards_next_stage_pointer(sample_message: PipelineMessage) -> None:
    producer = FakeProducer()

    def process(msg: PipelineMessage) -> PipelineMessage:
        return msg.to_next_stage(b2_path="dedup/x.parquet")

    runner = WorkerRunner(
        settings=_settings(),
        consumer=FakeConsumer([]),
        producer=producer,
        process=process,
    )
    runner.handle_one(serialize_message(sample_message))

    assert len(producer.sent) == 1
    topic, value = producer.sent[0]
    assert topic == "out"
    assert parse_message(value).b2_path == "dedup/x.parquet"


def test_handle_one_routes_exceptions_to_dlq(sample_message: PipelineMessage) -> None:
    producer = FakeProducer()

    def boom(msg: PipelineMessage) -> PipelineMessage:
        raise RuntimeError("kaboom")

    runner = WorkerRunner(
        settings=_settings(),
        consumer=FakeConsumer([]),
        producer=producer,
        process=boom,
    )
    runner.handle_one(serialize_message(sample_message))

    assert len(producer.sent) == 1
    topic, value = producer.sent[0]
    assert topic == DLQ_TOPIC
    record = json.loads(value)
    assert record["error_class"] == "RuntimeError"
    assert record["error_message"] == "kaboom"
    assert record["original"]["batch_id"] == sample_message.batch_id
    assert "Traceback" in record["error_traceback"]


def test_handle_one_is_idempotent_on_batch_id(sample_message: PipelineMessage) -> None:
    producer = FakeProducer()
    call_count = {"n": 0}

    def process(msg: PipelineMessage) -> PipelineMessage:
        call_count["n"] += 1
        return msg.to_next_stage(b2_path="dedup/x.parquet")

    runner = WorkerRunner(
        settings=_settings(),
        consumer=FakeConsumer([]),
        producer=producer,
        process=process,
    )
    runner.handle_one(serialize_message(sample_message))
    runner.handle_one(serialize_message(sample_message))

    assert call_count["n"] == 1
    assert len(producer.sent) == 1  # second delivery skipped


def test_run_forever_consumes_multiple_messages(sample_message: PipelineMessage) -> None:
    producer = FakeProducer()
    seen: list[str] = []

    def process(msg: PipelineMessage) -> PipelineMessage:
        seen.append(msg.batch_id)
        return msg.to_next_stage(b2_path=f"dedup/{msg.batch_id}.parquet")

    m1 = sample_message
    m2 = PipelineMessage(batch_id="batch-002", source="s", b2_path="raw/y", record_count=3)

    runner = WorkerRunner(
        settings=_settings(),
        consumer=FakeConsumer([serialize_message(m1), serialize_message(m2)]),
        producer=producer,
        process=process,
    )
    runner.run_forever()

    assert seen == ["batch-001", "batch-002"]
    assert len(producer.sent) == 2


def test_no_produce_when_produce_topic_is_none(sample_message: PipelineMessage) -> None:
    """Terminal workers (ingest) set produce_topic=None and must not publish."""
    producer = FakeProducer()
    terminal_settings = WorkerSettings(
        worker_name="terminal",
        consume_topic="in",
        produce_topic=None,
        consumer_group="g",
    )

    def process(msg: PipelineMessage) -> None:
        return None

    runner = WorkerRunner(
        settings=terminal_settings,
        consumer=FakeConsumer([]),
        producer=producer,
        process=process,
    )
    runner.handle_one(serialize_message(sample_message))

    assert producer.sent == []


def test_send_failure_after_mark_does_not_record_checkpoint_inmemory(
    sample_message: PipelineMessage,
) -> None:
    """Regression for #43: if ``producer.send`` raises, the batch must NOT
    be marked, so a restart reprocesses and re-sends it instead of silently
    skipping a lost downstream message.

    Covers the default ``InMemoryCheckpoint`` path.
    """
    producer = _SendFailsProducer(failing_topic="out")
    checkpoint = InMemoryCheckpoint()

    def process(msg: PipelineMessage) -> PipelineMessage:
        return msg.to_next_stage(b2_path="dedup/x.parquet")

    runner = WorkerRunner(
        settings=_settings(),
        consumer=FakeConsumer([]),
        producer=producer,
        process=process,
        checkpoint=checkpoint,
    )

    # The send raises out of handle_one — the batch was not successfully
    # produced, so it must not be recorded as processed.
    with pytest.raises(RuntimeError, match="broker unavailable"):
        runner.handle_one(serialize_message(sample_message))

    assert checkpoint.seen(sample_message.batch_id) is False
    assert producer.sent == []  # nothing landed downstream


def test_send_failure_after_mark_does_not_record_checkpoint_pg(
    sample_message: PipelineMessage,
) -> None:
    """Regression for #43 on the durable ``PgCheckpoint`` path.

    A mark-before-send ordering would leave a permanent row in
    ``pipeline.worker_checkpoint`` for a batch that never produced its
    downstream message — exactly the durable-loss this issue describes.
    """
    producer = _SendFailsProducer(failing_topic="out")
    conn = _FakePgConnection()
    checkpoint = PgCheckpoint(conn=conn, stage="dedup-worker")

    def process(msg: PipelineMessage) -> PipelineMessage:
        return msg.to_next_stage(b2_path="dedup/x.parquet")

    runner = WorkerRunner(
        settings=_settings(),
        consumer=FakeConsumer([]),
        producer=producer,
        process=process,
        checkpoint=checkpoint,
    )

    with pytest.raises(RuntimeError, match="broker unavailable"):
        runner.handle_one(serialize_message(sample_message))

    assert checkpoint.seen(sample_message.batch_id) is False
    assert producer.sent == []


def test_successful_send_then_marks_checkpoint(
    sample_message: PipelineMessage,
) -> None:
    """The happy path still marks: send succeeds, then the batch is recorded
    so a redelivery is correctly skipped."""
    producer = FakeProducer()
    checkpoint = InMemoryCheckpoint()

    def process(msg: PipelineMessage) -> PipelineMessage:
        return msg.to_next_stage(b2_path="dedup/x.parquet")

    runner = WorkerRunner(
        settings=_settings(),
        consumer=FakeConsumer([]),
        producer=producer,
        process=process,
        checkpoint=checkpoint,
    )
    runner.handle_one(serialize_message(sample_message))

    assert checkpoint.seen(sample_message.batch_id) is True
    assert len(producer.sent) == 1
