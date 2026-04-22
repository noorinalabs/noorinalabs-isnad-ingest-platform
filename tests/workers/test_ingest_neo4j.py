"""Unit tests for the Neo4j-backed ingest processor (issues #13, #18, #192 D-ii).

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
    ManifestMissingError,
    UnknownSchemaError,
    _build_edge_cypher,
    _build_node_cypher,
)
from workers.ingest.schema import NODE_PROPERTY_MAP
from workers.lib.dlq import DLQ_TOPIC
from workers.lib.message import PipelineMessage, serialize_message
from workers.lib.object_store import ObjectStore
from workers.lib.runner import WorkerRunner, WorkerSettings
from workers.lib.topics import PIPELINE_NORMALIZE_DONE

# ---------------------------------------------------------------------------
# Neo4j driver fakes — capture every tx.run call in order
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
# D-ii fixture helpers — write a folder-prefix manifest with per-label Parquets
# ---------------------------------------------------------------------------

_NODE_SCHEMA = pa.schema(
    [
        pa.field("label", pa.string(), nullable=False),
        pa.field("id", pa.string(), nullable=False),
        pa.field("props", pa.string(), nullable=False),
    ]
)

_EDGE_SCHEMA = pa.schema(
    [
        pa.field("label", pa.string(), nullable=False),
        pa.field("src_id", pa.string(), nullable=False),
        pa.field("src_label", pa.string(), nullable=False),
        pa.field("dst_id", pa.string(), nullable=False),
        pa.field("dst_label", pa.string(), nullable=False),
        pa.field("props", pa.string(), nullable=False),
    ]
)


def _node_parquet(label: str, rows: list[dict[str, Any]]) -> bytes:
    """Build a per-label nodes parquet matching normalize's output shape."""
    if not rows:
        table = _NODE_SCHEMA.empty_table()
    else:
        table = pa.table(
            {
                "label": [label] * len(rows),
                "id": [r["id"] for r in rows],
                "props": [json.dumps(r.get("props", {}), ensure_ascii=False) for r in rows],
            },
            schema=_NODE_SCHEMA,
        )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _edges_parquet(rows: list[dict[str, Any]]) -> bytes:
    if not rows:
        table = _EDGE_SCHEMA.empty_table()
    else:
        table = pa.table(
            {
                "label": [r["label"] for r in rows],
                "src_id": [r["src_id"] for r in rows],
                "src_label": [r["src_label"] for r in rows],
                "dst_id": [r["dst_id"] for r in rows],
                "dst_label": [r["dst_label"] for r in rows],
                "props": [json.dumps(r.get("props", {}), ensure_ascii=False) for r in rows],
            },
            schema=_EDGE_SCHEMA,
        )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _write_batch(
    store: ObjectStore,
    prefix: str,
    *,
    nodes: dict[str, list[dict[str, Any]]] | None = None,
    edges: list[dict[str, Any]] | None = None,
    extra_manifest_entries: list[dict[str, Any]] | None = None,
    skip_keys: set[str] | None = None,
    skip_manifest: bool = False,
) -> None:
    """Write a D-ii-shaped batch to the fake object store.

    ``skip_keys`` names files listed in the manifest that should NOT be
    written (used to simulate a missing Parquet). ``skip_manifest``
    simulates an absent manifest.
    """
    nodes = nodes or {}
    edges = edges or []
    skip_keys = skip_keys or set()

    manifest_entries: list[dict[str, Any]] = []
    total = 0
    for label, rows in nodes.items():
        filename = f"{label.lower()}s.parquet"
        key = f"{prefix}{filename}"
        if key not in skip_keys:
            store.put_object(key, _node_parquet(label, rows))
        manifest_entries.append({"path": filename, "label": label, "row_count": len(rows)})
        total += len(rows)

    edges_key = f"{prefix}edges.parquet"
    if edges_key not in skip_keys:
        store.put_object(edges_key, _edges_parquet(edges))
    manifest_entries.append({"path": "edges.parquet", "row_count": len(edges)})
    total += len(edges)

    if extra_manifest_entries:
        manifest_entries.extend(extra_manifest_entries)

    if not skip_manifest:
        manifest = {
            "batch_id": "batch-001",
            "source": "sunnah-api",
            "created_at": "2026-04-21T00:00:00+00:00",
            "parquets": manifest_entries,
            "total_row_count": total,
        }
        store.put_object(f"{prefix}_MANIFEST.json", json.dumps(manifest).encode("utf-8"))


@pytest.fixture
def folder_prefix_message() -> PipelineMessage:
    """D-ii message: ``b2_path`` is a folder prefix with trailing slash."""
    return PipelineMessage(
        batch_id="batch-001",
        source="sunnah-api",
        b2_path="normalized/batch-001/",
        record_count=0,  # total_row_count is carried in the manifest
    )


# ---------------------------------------------------------------------------
# Cypher builder — per-field SET shape
# ---------------------------------------------------------------------------


def test_node_cypher_uses_per_field_set_not_plus_equals() -> None:
    """Phase-4 safety: no ``SET n += row.props`` anywhere."""
    query = _build_node_cypher("Hadith")
    assert "SET n += row.props" not in query
    assert "MERGE (n:`Hadith` {id: row.id})" in query
    # Every allow-listed field appears as a coalesce-guarded SET stanza
    # so a row that omits the field does not wipe an existing value.
    for field in NODE_PROPERTY_MAP["Hadith"]:
        assert f"n.{field} = coalesce(row.props.{field}, n.{field})" in query


def test_node_cypher_for_empty_allowlist_has_no_set() -> None:
    """A label with no allow-listed props still MERGEs on id, no SET."""

    # All current labels have at least one field, so we test via a
    # fabricated label instead by temporarily referencing the builder's
    # fallback path through a label that returns [] from the map.
    # _build_node_cypher uses .get(label, []) so an absent label maps to [].
    query = _build_node_cypher("__NonexistentForTest__")
    assert "MERGE" in query
    assert "SET" not in query


def test_edge_cypher_matches_endpoints_then_merges() -> None:
    query = _build_edge_cypher("TRANSMITTED_TO")
    assert "MATCH (s {id: row.src_id})" in query
    assert "MATCH (t {id: row.dst_id})" in query
    assert "MERGE (s)-[r:`TRANSMITTED_TO`]->(t)" in query
    assert (
        "r.position_in_chain = coalesce(row.props.position_in_chain, r.position_in_chain)" in query
    )
    assert "SET r += row.props" not in query


# ---------------------------------------------------------------------------
# Happy path — manifest-gated MERGE
# ---------------------------------------------------------------------------


def test_ingest_reads_manifest_and_merges_nodes_then_edges(
    object_store: ObjectStore, folder_prefix_message: PipelineMessage
) -> None:
    prefix = folder_prefix_message.b2_path
    _write_batch(
        object_store,
        prefix,
        nodes={
            "Narrator": [
                {"id": "nar:a", "props": {"name_en": "A"}},
                {"id": "nar:b", "props": {"name_en": "B"}},
            ],
            "Hadith": [{"id": "hdt:1", "props": {"matn_ar": "..."}}],
            "Collection": [{"id": "col:bukhari", "props": {"name_en": "Bukhari"}}],
        },
        edges=[
            {
                "label": "TRANSMITTED_TO",
                "src_id": "nar:a",
                "src_label": "Narrator",
                "dst_id": "nar:b",
                "dst_label": "Narrator",
                "props": {"position_in_chain": 0},
            },
            {
                "label": "APPEARS_IN",
                "src_id": "hdt:1",
                "src_label": "Hadith",
                "dst_id": "col:bukhari",
                "dst_label": "Collection",
                "props": {"book_number": 1},
            },
        ],
    )
    driver = _FakeDriver()

    IngestProcessor(object_store, neo4j_driver=driver)(folder_prefix_message)

    # 3 node labels + 2 edge labels = 5 tx.run calls
    assert len(driver.sink) == 5
    queries = [q for q, _ in driver.sink]
    node_idx = {
        label: next(i for i, q in enumerate(queries) if f"MERGE (n:`{label}`" in q)
        for label in ("Narrator", "Hadith", "Collection")
    }
    edge_idx = {
        label: next(i for i, q in enumerate(queries) if f"MERGE (s)-[r:`{label}`]" in q)
        for label in ("TRANSMITTED_TO", "APPEARS_IN")
    }
    # Ordering contract: every edge MERGE comes after every node MERGE
    assert max(node_idx.values()) < min(edge_idx.values())


def test_ingest_empty_batch_is_noop(
    object_store: ObjectStore, folder_prefix_message: PipelineMessage
) -> None:
    _write_batch(object_store, folder_prefix_message.b2_path, nodes={}, edges=[])
    driver = _FakeDriver()

    IngestProcessor(object_store, neo4j_driver=driver)(folder_prefix_message)

    assert driver.sink == []


def test_ingest_without_driver_is_readonly(
    object_store: ObjectStore, folder_prefix_message: PipelineMessage
) -> None:
    """Read-path ergonomics: no driver → parse manifest + parquets, skip write."""
    _write_batch(
        object_store,
        folder_prefix_message.b2_path,
        nodes={"Hadith": [{"id": "hdt:1", "props": {"matn_ar": "..."}}]},
    )

    # No assertion on Neo4j; absence of exception is the contract.
    IngestProcessor(object_store, neo4j_driver=None)(folder_prefix_message)


def test_ingest_normalizes_prefix_without_trailing_slash(
    object_store: ObjectStore,
) -> None:
    """A defensive callsite may strip the trailing slash — ingest restores it."""
    msg = PipelineMessage(
        batch_id="batch-001",
        source="sunnah-api",
        b2_path="normalized/batch-001",  # note: no trailing slash
        record_count=0,
    )
    _write_batch(
        object_store,
        "normalized/batch-001/",
        nodes={"Hadith": [{"id": "hdt:1", "props": {"matn_ar": "x"}}]},
    )
    driver = _FakeDriver()

    IngestProcessor(object_store, neo4j_driver=driver)(msg)

    assert len(driver.sink) == 1  # one MERGE for Hadith


# ---------------------------------------------------------------------------
# Manifest gating — error taxonomy
# ---------------------------------------------------------------------------


def test_missing_manifest_raises_manifest_missing_error(
    object_store: ObjectStore, folder_prefix_message: PipelineMessage
) -> None:
    _write_batch(
        object_store,
        folder_prefix_message.b2_path,
        nodes={"Hadith": [{"id": "hdt:1", "props": {}}]},
        skip_manifest=True,
    )
    driver = _FakeDriver()

    with pytest.raises(ManifestMissingError):
        IngestProcessor(object_store, neo4j_driver=driver)(folder_prefix_message)
    # ManifestMissingError is a subclass of UnknownSchemaError so the
    # runner's existing schema-error branch DLQs it.
    assert driver.sink == []


def test_malformed_manifest_raises_manifest_missing_error(
    object_store: ObjectStore, folder_prefix_message: PipelineMessage
) -> None:
    object_store.put_object(
        f"{folder_prefix_message.b2_path}_MANIFEST.json",
        b"this is not valid json",
    )

    with pytest.raises(ManifestMissingError, match="not valid JSON"):
        IngestProcessor(object_store, neo4j_driver=_FakeDriver())(folder_prefix_message)


def test_missing_expected_parquet_raises_schema_error(
    object_store: ObjectStore, folder_prefix_message: PipelineMessage
) -> None:
    prefix = folder_prefix_message.b2_path
    _write_batch(
        object_store,
        prefix,
        nodes={"Hadith": [{"id": "hdt:1", "props": {"matn_ar": "..."}}]},
        skip_keys={f"{prefix}hadiths.parquet"},
    )
    driver = _FakeDriver()

    with pytest.raises(UnknownSchemaError, match="Hadith parquet"):
        IngestProcessor(object_store, neo4j_driver=driver)(folder_prefix_message)
    assert driver.sink == []


def test_unknown_label_in_manifest_raises_schema_error(
    object_store: ObjectStore, folder_prefix_message: PipelineMessage
) -> None:
    _write_batch(
        object_store,
        folder_prefix_message.b2_path,
        nodes={},
        extra_manifest_entries=[{"path": "evil.parquet", "label": "Evil", "row_count": 0}],
    )

    with pytest.raises(UnknownSchemaError, match="unknown node label"):
        IngestProcessor(object_store, neo4j_driver=_FakeDriver())(folder_prefix_message)


def test_unknown_edge_label_in_batch_raises_schema_error(
    object_store: ObjectStore, folder_prefix_message: PipelineMessage
) -> None:
    _write_batch(
        object_store,
        folder_prefix_message.b2_path,
        nodes={"Hadith": [{"id": "hdt:1", "props": {}}]},
        edges=[
            {
                "label": "SUBVERT_GRAPH",
                "src_id": "hdt:1",
                "src_label": "Hadith",
                "dst_id": "hdt:1",
                "dst_label": "Hadith",
                "props": {},
            }
        ],
    )
    driver = _FakeDriver()

    with pytest.raises(UnknownSchemaError, match="unknown label 'SUBVERT_GRAPH'"):
        IngestProcessor(object_store, neo4j_driver=driver)(folder_prefix_message)
    assert driver.sink == []  # schema error surfaced before Neo4j


# ---------------------------------------------------------------------------
# Phase-4 safety — per-field SET semantics
# ---------------------------------------------------------------------------


def test_ingest_sets_only_fields_present_in_row_props(
    object_store: ObjectStore, folder_prefix_message: PipelineMessage
) -> None:
    """A row that omits ``matn_ar`` must NOT wipe an existing node's ``matn_ar``.

    The generated Cypher uses ``coalesce(row.props.matn_ar, n.matn_ar)``
    so when the row omits ``matn_ar`` (serialized as NULL), coalesce
    falls back to the existing node value. Scholar-curated fields that
    the normalize batch doesn't know about are therefore preserved.
    """
    _write_batch(
        object_store,
        folder_prefix_message.b2_path,
        nodes={"Hadith": [{"id": "hdt:1", "props": {"matn_en": "foo"}}]},
    )
    driver = _FakeDriver()

    IngestProcessor(object_store, neo4j_driver=driver)(folder_prefix_message)

    assert len(driver.sink) == 1
    query, params = driver.sink[0]
    sent_props = params["rows"][0]["props"]
    assert sent_props == {"matn_en": "foo"}
    # matn_en SET is a straight assignment (row has a value)
    assert "n.matn_en = coalesce(row.props.matn_en, n.matn_en)" in query
    # matn_ar is still in the SET list (Cypher is built from the
    # per-label allow-list, not the row), BUT the coalesce protects
    # the existing node value from being wiped with NULL.
    assert "n.matn_ar = coalesce(row.props.matn_ar, n.matn_ar)" in query


def test_ingest_drops_properties_outside_allowlist(
    object_store: ObjectStore, folder_prefix_message: PipelineMessage
) -> None:
    """An attacker-controlled ``evil`` key must be silently dropped."""
    _write_batch(
        object_store,
        folder_prefix_message.b2_path,
        nodes={
            "Hadith": [
                {
                    "id": "hdt:1",
                    "props": {
                        "matn_en": "legitimate",
                        "evil": "bad",
                        "label": "Administrator",
                    },
                }
            ]
        },
    )
    driver = _FakeDriver()

    IngestProcessor(object_store, neo4j_driver=driver)(folder_prefix_message)

    assert len(driver.sink) == 1
    query, params = driver.sink[0]
    sent_props = params["rows"][0]["props"]
    assert sent_props == {"matn_en": "legitimate"}
    assert "evil" not in query
    # The "label" key (not allow-listed for Hadith) is dropped.
    # The allow-list also has no "label" field, so no SET stanza.
    assert "n.label =" not in query
    assert "row.props.evil" not in query


# ---------------------------------------------------------------------------
# Prior error taxonomy — preserved under the new signature
# ---------------------------------------------------------------------------


def test_ingest_retries_once_on_session_expired(
    object_store: ObjectStore, folder_prefix_message: PipelineMessage
) -> None:
    from neo4j.exceptions import SessionExpired

    _write_batch(
        object_store,
        folder_prefix_message.b2_path,
        nodes={"Narrator": [{"id": "nar:a", "props": {"name_en": "A"}}]},
    )
    first = _FakeSession([], raise_on_execute=SessionExpired("token expired"))
    second = _FakeSession([])
    driver = _FakeDriver(session_factories=[first, second])

    IngestProcessor(object_store, neo4j_driver=driver)(folder_prefix_message)

    # Reopened — the successful second session ran the full batch
    assert driver._next_factory == 2
    assert len(driver.sink) == 1  # one tx.run for the single Narrator label


def test_ingest_client_error_propagates_for_dlq(
    object_store: ObjectStore, folder_prefix_message: PipelineMessage
) -> None:
    """Cypher/constraint errors must propagate so the runner DLQs them."""
    from neo4j.exceptions import ClientError

    _write_batch(
        object_store,
        folder_prefix_message.b2_path,
        nodes={"Narrator": [{"id": "nar:a", "props": {"name_en": "A"}}]},
    )
    sess = _FakeSession([], raise_on_execute=ClientError("constraint violated"))
    driver = _FakeDriver(session_factories=[sess])

    with pytest.raises(ClientError):
        IngestProcessor(object_store, neo4j_driver=driver)(folder_prefix_message)


# ---------------------------------------------------------------------------
# Runner integration — DLQ routing
# ---------------------------------------------------------------------------


def test_runner_routes_client_error_to_dlq(
    object_store: ObjectStore, folder_prefix_message: PipelineMessage
) -> None:
    from neo4j.exceptions import ClientError

    _write_batch(
        object_store,
        folder_prefix_message.b2_path,
        nodes={"Narrator": [{"id": "nar:a", "props": {"name_en": "A"}}]},
    )
    sess = _FakeSession([], raise_on_execute=ClientError("bad query"))
    driver = _FakeDriver(session_factories=[sess])
    producer = FakeProducer()

    runner = WorkerRunner(
        settings=WorkerSettings(
            worker_name="ingest-worker",
            consume_topic=PIPELINE_NORMALIZE_DONE,
            produce_topic=None,
            consumer_group="ingest-worker",
        ),
        consumer=FakeConsumer([]),
        producer=producer,
        process=IngestProcessor(object_store, neo4j_driver=driver),
    )
    runner.handle_one(serialize_message(folder_prefix_message))

    assert len(producer.sent) == 1
    topic, value = producer.sent[0]
    assert topic == DLQ_TOPIC
    record = json.loads(value)
    assert record["error_class"] == "ClientError"


def test_runner_routes_schema_error_to_dlq(
    object_store: ObjectStore, folder_prefix_message: PipelineMessage
) -> None:
    _write_batch(
        object_store,
        folder_prefix_message.b2_path,
        nodes={},
        extra_manifest_entries=[{"path": "bad.parquet", "label": "BadLabel", "row_count": 0}],
    )
    driver = _FakeDriver()
    producer = FakeProducer()

    runner = WorkerRunner(
        settings=WorkerSettings(
            worker_name="ingest-worker",
            consume_topic=PIPELINE_NORMALIZE_DONE,
            produce_topic=None,
            consumer_group="ingest-worker",
        ),
        consumer=FakeConsumer([]),
        producer=producer,
        process=IngestProcessor(object_store, neo4j_driver=driver),
    )
    runner.handle_one(serialize_message(folder_prefix_message))

    assert len(producer.sent) == 1
    topic, value = producer.sent[0]
    assert topic == DLQ_TOPIC
    record = json.loads(value)
    assert record["error_class"] == "UnknownSchemaError"
    assert driver.sink == []


def test_runner_routes_missing_manifest_to_dlq(
    object_store: ObjectStore, folder_prefix_message: PipelineMessage
) -> None:
    # No batch written at all — manifest missing
    driver = _FakeDriver()
    producer = FakeProducer()

    runner = WorkerRunner(
        settings=WorkerSettings(
            worker_name="ingest-worker",
            consume_topic=PIPELINE_NORMALIZE_DONE,
            produce_topic=None,
            consumer_group="ingest-worker",
        ),
        consumer=FakeConsumer([]),
        producer=producer,
        process=IngestProcessor(object_store, neo4j_driver=driver),
    )
    runner.handle_one(serialize_message(folder_prefix_message))

    assert len(producer.sent) == 1
    topic, value = producer.sent[0]
    assert topic == DLQ_TOPIC
    record = json.loads(value)
    assert record["error_class"] == "ManifestMissingError"
