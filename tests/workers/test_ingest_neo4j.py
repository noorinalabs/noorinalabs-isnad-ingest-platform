"""Unit tests for the Neo4j-backed ingest processor (issue #13).

The tests mock the neo4j driver/session so the suite runs without
Docker or a live database. Integration coverage against a real Neo4j
(testcontainers) is tracked separately in issue #136.
"""

from __future__ import annotations

import io
import json
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests.workers.conftest import FakeConsumer, FakeProducer
from workers.ingest.processor import (
    IngestProcessor,
    UnknownSchemaError,
    _merge_node_tx,
)
from workers.lib.dlq import DLQ_TOPIC
from workers.lib.message import PipelineMessage, serialize_message
from workers.lib.object_store import ObjectStore
from workers.lib.runner import WorkerRunner, WorkerSettings

# ---------------------------------------------------------------------------
# Neo4j driver fakes
# ---------------------------------------------------------------------------


class _FakeResult:
    def __init__(self, record: dict[str, Any] | None) -> None:
        self._record = record

    def single(self) -> dict[str, Any] | None:
        return self._record


class _FakeTx:
    def __init__(self, sink: list[tuple[str, dict[str, Any]]]) -> None:
        self._sink = sink

    def run(self, query: str, **params: Any) -> _FakeResult:
        self._sink.append((query, params))
        rows = params.get("rows") or []
        return _FakeResult({"merged": len(rows)})


class _FakeSession:
    def __init__(
        self,
        sink: list[tuple[str, dict[str, Any]]],
        *,
        raise_on_execute: Exception | None = None,
    ) -> None:
        self._sink = sink
        self._raise = raise_on_execute

    def __enter__(self) -> _FakeSession:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute_write(self, fn: Any, **kwargs: Any) -> Any:
        if self._raise is not None:
            err = self._raise
            self._raise = None  # one-shot: subsequent calls succeed
            raise err
        tx = _FakeTx(self._sink)
        return fn(tx, **kwargs)


class _FakeDriver:
    def __init__(self, *, session_factories: list[_FakeSession] | None = None) -> None:
        self.sink: list[tuple[str, dict[str, Any]]] = []
        self.closed = False
        self._factories = session_factories or []
        self._next_factory = 0

    def session(self) -> _FakeSession:
        if self._factories:
            if self._next_factory >= len(self._factories):
                msg = "No more fake sessions configured"
                raise AssertionError(msg)
            sess = self._factories[self._next_factory]
            self._next_factory += 1
            sess._sink = self.sink  # share recording sink
            return sess
        return _FakeSession(self.sink)

    def close(self) -> None:
        self.closed = True


# ---------------------------------------------------------------------------
# Parquet payload helpers
# ---------------------------------------------------------------------------


def _parquet_bytes(rows: list[dict[str, Any]]) -> bytes:
    if not rows:
        table = pa.table({"label": [], "id": []})
    else:
        columns: dict[str, list[Any]] = {}
        for row in rows:
            for k, v in row.items():
                columns.setdefault(k, [])
        for row in rows:
            for k in columns:
                columns[k].append(row.get(k))
        table = pa.table(columns)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# _merge_node_tx — Cypher shape
# ---------------------------------------------------------------------------


def test_merge_node_tx_emits_parameterized_merge() -> None:
    sink: list[tuple[str, dict[str, Any]]] = []
    tx = _FakeTx(sink)
    rows = [
        {"id": "nar:ibn-abbas", "props": {"name_en": "Ibn Abbas"}},
        {"id": "nar:abu-hurayra", "props": {"name_en": "Abu Hurayra"}},
    ]

    count = _merge_node_tx(tx, label="Narrator", rows=rows)

    assert count == 2
    assert len(sink) == 1
    query, params = sink[0]
    assert "MERGE (n:`Narrator` {id: row.id})" in query
    assert "SET n += row.props" in query
    assert params["rows"] == rows


# ---------------------------------------------------------------------------
# IngestProcessor — happy path
# ---------------------------------------------------------------------------


def test_ingest_opens_session_and_merges_by_label(
    object_store: ObjectStore, sample_message: PipelineMessage
) -> None:
    payload = _parquet_bytes(
        [
            {"label": "Narrator", "id": "nar:a", "name_en": "A"},
            {"label": "Narrator", "id": "nar:b", "name_en": "B"},
            {"label": "Hadith", "id": "hdt:1", "matn_ar": "..."},
        ]
    )
    object_store.put_object(sample_message.b2_path, payload)
    driver = _FakeDriver()

    result = IngestProcessor(object_store, neo4j_driver=driver)(sample_message)

    assert result is None  # terminal stage
    # One MERGE statement per distinct label → 2 tx.run calls
    assert len(driver.sink) == 2
    queries = [q for q, _ in driver.sink]
    assert any("MERGE (n:`Narrator` {id: row.id})" in q for q in queries)
    assert any("MERGE (n:`Hadith` {id: row.id})" in q for q in queries)
    # Narrator batch carries both rows with props stripped of label/id
    narrator_batch = next(params["rows"] for q, params in driver.sink if "Narrator" in q)
    assert {r["id"] for r in narrator_batch} == {"nar:a", "nar:b"}
    for row in narrator_batch:
        assert "label" not in row["props"]
        assert "id" not in row["props"]


def test_ingest_empty_parquet_is_noop(
    object_store: ObjectStore, sample_message: PipelineMessage
) -> None:
    object_store.put_object(sample_message.b2_path, _parquet_bytes([]))
    driver = _FakeDriver()

    IngestProcessor(object_store, neo4j_driver=driver)(sample_message)

    assert driver.sink == []


def test_ingest_without_driver_is_readonly(
    object_store: ObjectStore, sample_message: PipelineMessage
) -> None:
    """Unit-suite ergonomics: no driver → read payload, skip write."""
    object_store.put_object(sample_message.b2_path, _parquet_bytes([]))

    # No assertion on Neo4j; absence of exception is the contract.
    IngestProcessor(object_store, neo4j_driver=None)(sample_message)


# ---------------------------------------------------------------------------
# IngestProcessor — error taxonomy
# ---------------------------------------------------------------------------


def test_ingest_unknown_label_raises_schema_error(
    object_store: ObjectStore, sample_message: PipelineMessage
) -> None:
    payload = _parquet_bytes([{"label": "Evil", "id": "x:1"}])
    object_store.put_object(sample_message.b2_path, payload)
    driver = _FakeDriver()

    with pytest.raises(UnknownSchemaError, match="unknown label"):
        IngestProcessor(object_store, neo4j_driver=driver)(sample_message)

    assert driver.sink == []  # no Neo4j call attempted


def test_ingest_missing_id_raises_schema_error(
    object_store: ObjectStore, sample_message: PipelineMessage
) -> None:
    payload = _parquet_bytes([{"label": "Narrator", "id": None}])
    object_store.put_object(sample_message.b2_path, payload)
    driver = _FakeDriver()

    with pytest.raises(UnknownSchemaError, match="'id'"):
        IngestProcessor(object_store, neo4j_driver=driver)(sample_message)


def test_ingest_retries_once_on_session_expired(
    object_store: ObjectStore, sample_message: PipelineMessage
) -> None:
    from neo4j.exceptions import SessionExpired

    payload = _parquet_bytes([{"label": "Narrator", "id": "nar:a", "name_en": "A"}])
    object_store.put_object(sample_message.b2_path, payload)
    # First session raises SessionExpired on execute_write; second succeeds.
    first = _FakeSession([], raise_on_execute=SessionExpired("token expired"))
    second = _FakeSession([])
    driver = _FakeDriver(session_factories=[first, second])

    IngestProcessor(object_store, neo4j_driver=driver)(sample_message)

    # Reopened — one successful tx.run on the second session
    assert len(driver.sink) == 1
    assert driver._next_factory == 2


def test_ingest_client_error_propagates_for_dlq(
    object_store: ObjectStore, sample_message: PipelineMessage
) -> None:
    """Cypher/constraint errors must propagate so the runner DLQs them."""
    from neo4j.exceptions import ClientError

    payload = _parquet_bytes([{"label": "Narrator", "id": "nar:a", "name_en": "A"}])
    object_store.put_object(sample_message.b2_path, payload)
    sess = _FakeSession([], raise_on_execute=ClientError("constraint violated"))
    driver = _FakeDriver(session_factories=[sess])

    with pytest.raises(ClientError):
        IngestProcessor(object_store, neo4j_driver=driver)(sample_message)


# ---------------------------------------------------------------------------
# Runner integration — DLQ routing on Neo4j failures
# ---------------------------------------------------------------------------


def test_runner_routes_client_error_to_dlq(
    object_store: ObjectStore, sample_message: PipelineMessage
) -> None:
    from neo4j.exceptions import ClientError

    payload = _parquet_bytes([{"label": "Narrator", "id": "nar:a", "name_en": "A"}])
    object_store.put_object(sample_message.b2_path, payload)
    sess = _FakeSession([], raise_on_execute=ClientError("bad query"))
    driver = _FakeDriver(session_factories=[sess])
    producer = FakeProducer()

    runner = WorkerRunner(
        settings=WorkerSettings(
            worker_name="ingest-worker",
            consume_topic="pipeline.norm.done",
            produce_topic=None,
            consumer_group="ingest-worker",
        ),
        consumer=FakeConsumer([]),
        producer=producer,
        process=IngestProcessor(object_store, neo4j_driver=driver),
    )
    runner.handle_one(serialize_message(sample_message))

    assert len(producer.sent) == 1
    topic, value = producer.sent[0]
    assert topic == DLQ_TOPIC
    record = json.loads(value)
    assert record["error_class"] == "ClientError"


def test_runner_routes_schema_error_to_dlq(
    object_store: ObjectStore, sample_message: PipelineMessage
) -> None:
    payload = _parquet_bytes([{"label": "BadLabel", "id": "x:1"}])
    object_store.put_object(sample_message.b2_path, payload)
    driver = _FakeDriver()
    producer = FakeProducer()

    runner = WorkerRunner(
        settings=WorkerSettings(
            worker_name="ingest-worker",
            consume_topic="pipeline.norm.done",
            produce_topic=None,
            consumer_group="ingest-worker",
        ),
        consumer=FakeConsumer([]),
        producer=producer,
        process=IngestProcessor(object_store, neo4j_driver=driver),
    )
    runner.handle_one(serialize_message(sample_message))

    assert len(producer.sent) == 1
    topic, value = producer.sent[0]
    assert topic == DLQ_TOPIC
    record = json.loads(value)
    assert record["error_class"] == "UnknownSchemaError"
    assert driver.sink == []
