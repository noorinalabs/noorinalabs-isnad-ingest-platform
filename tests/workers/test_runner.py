"""WorkerRunner behaviour: success, DLQ routing, idempotency."""

from __future__ import annotations

import json

from tests.workers.conftest import FakeConsumer, FakeProducer
from workers.lib.dlq import DLQ_TOPIC
from workers.lib.message import PipelineMessage, parse_message, serialize_message
from workers.lib.runner import WorkerRunner, WorkerSettings


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
