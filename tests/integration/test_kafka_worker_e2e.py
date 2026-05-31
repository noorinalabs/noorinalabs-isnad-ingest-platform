"""Kafka-topic-driven worker E2E across enrich → normalize → ingest (#55 gap 1).

The unit suite tests each processor class with in-memory inputs. This module
drives the workers **through real Kafka topics** end to end: a seed message is
produced to the enrich stage's input topic, and each ``WorkerRunner``
consumes from its real Kafka topic, runs the production processor, writes to a
real MinIO object store, and produces to the next stage's real topic. The
terminal ingest stage MERGEs into a real Neo4j container.

What is genuinely exercised here (vs the unit suite)
----------------------------------------------------
* The Kafka wire: ``KafkaConsumer`` group subscription + offset commit and
  ``KafkaProducer`` publish across ``pipeline.dedup.done`` →
  ``pipeline.enrich.done`` → ``pipeline.normalize.done`` topics.
* The production entrypoints: ``test_main_build_runner_wires_against_live_infra``
  calls each stage's actual ``workers.<stage>.main.build_runner()`` against the
  live Kafka/MinIO/Neo4j containers to prove the env-driven wiring connects
  (the ingest entrypoint runs ``driver.verify_connectivity()`` at build time).
* The cross-stage object hand-off: enrich's pass-through copy → normalize's
  D-ii manifest fan-out → ingest's manifest-gated MERGE, all through one
  shared MinIO bucket.

Determinism note
----------------
The production ``build_runner`` consumers use the kafka-python default
``auto_offset_reset`` (``latest``) and block forever in ``run_forever`` — the
right behavior for a long-lived worker, the wrong one for a bounded test. The
stage runners here are built with the *same* ``WorkerRunner`` + production
processors but a test-only consumer config (``auto_offset_reset="earliest"``,
a ``consumer_timeout_ms`` so ``run_forever`` returns after draining). The flow
across real topics is unchanged; only the consumer's start-offset and
idle-timeout differ. The ``build_runner`` entrypoints themselves are covered
by the separate wiring test above.

ML deps are NOT required: enrich degrades gracefully (pass-through copy + empty
topics side-table) without ``transformers``/``torch``, so the chain reaches
normalize and ingest on the hadith payload alone.

Runtime requirement
-------------------
``@pytest.mark.integration`` — needs Docker for THREE containers (Kafka,
MinIO, Neo4j). Excluded from the default ``pytest -m "not integration"`` run
(CI / ``make check``); runs via ``make test-integration``. ``importorskip`` on
``testcontainers.kafka``/``testcontainers.minio``/``boto3``/``kafka`` keeps a
Docker-less lane collecting cleanly.
"""

from __future__ import annotations

import io
import json
from collections.abc import Iterator
from typing import Any

import pyarrow.parquet as pq
import pytest

pytest.importorskip("testcontainers.kafka")
pytest.importorskip("testcontainers.minio")
pytest.importorskip("boto3")
pytest.importorskip("kafka")

import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from kafka import KafkaConsumer, KafkaProducer  # noqa: E402  # type: ignore[import-untyped]
from testcontainers.kafka import KafkaContainer  # noqa: E402
from testcontainers.minio import MinioContainer  # noqa: E402
from testcontainers.neo4j import Neo4jContainer  # noqa: E402

from tests.factories import build_hadith_table  # noqa: E402
from workers.enrich.processor import EnrichProcessor  # noqa: E402
from workers.ingest.processor import IngestProcessor  # noqa: E402
from workers.lib.message import PipelineMessage, serialize_message  # noqa: E402
from workers.lib.object_store import ObjectStore  # noqa: E402
from workers.lib.runner import WorkerRunner, WorkerSettings  # noqa: E402
from workers.lib.topics import (  # noqa: E402
    PIPELINE_DEDUP_DONE,
    PIPELINE_ENRICH_DONE,
    PIPELINE_NORMALIZE_DONE,
)
from workers.normalize.processor import NormalizeProcessor  # noqa: E402

pytestmark = pytest.mark.integration

TEST_BUCKET = "noorinalabs-pipeline"
NEO4J_TEST_PASSWORD = "testpassword123"
CONSUMER_TIMEOUT_MS = 20_000


# ---------------------------------------------------------------------------
# Container fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def kafka_container() -> Iterator[KafkaContainer]:
    container = KafkaContainer()
    with container as kafka:
        yield kafka


@pytest.fixture(scope="module")
def minio_container() -> Iterator[MinioContainer]:
    container = MinioContainer(access_key="minio", secret_key="minio12345")
    with container as minio:
        yield minio


@pytest.fixture(scope="module")
def neo4j_container_module() -> Iterator[Neo4jContainer]:
    container = Neo4jContainer("neo4j:5-community", password=NEO4J_TEST_PASSWORD)
    with container as neo4j:
        yield neo4j


@pytest.fixture
def bootstrap(kafka_container: KafkaContainer) -> str:
    return kafka_container.get_bootstrap_server()


@pytest.fixture
def minio_store(minio_container: MinioContainer) -> ObjectStore:
    """Real ``ObjectStore`` on the MinIO container, bucket pre-created."""
    cfg = minio_container.get_config()
    client = boto3.client(
        "s3",
        endpoint_url=f"http://{cfg['endpoint']}",
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    try:
        client.create_bucket(Bucket=TEST_BUCKET)
    except client.exceptions.BucketAlreadyOwnedByYou:
        pass
    return ObjectStore(bucket=TEST_BUCKET, client=client)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_stage_runner(
    *,
    bootstrap: str,
    store: ObjectStore,
    worker_name: str,
    consume_topic: str,
    produce_topic: str | None,
    process: Any,
) -> WorkerRunner:
    """Build a real-Kafka ``WorkerRunner`` configured for a bounded test drain.

    Mirrors ``workers.<stage>.main.build_runner`` (real KafkaConsumer/Producer
    + production processor) but with ``auto_offset_reset="earliest"`` and a
    ``consumer_timeout_ms`` so ``run_forever`` returns after the topic drains
    instead of blocking like a long-lived worker.
    """
    consumer = KafkaConsumer(
        consume_topic,
        bootstrap_servers=bootstrap,
        group_id=worker_name,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=CONSUMER_TIMEOUT_MS,
        value_deserializer=None,
    )
    producer = KafkaProducer(bootstrap_servers=bootstrap)
    return WorkerRunner(
        settings=WorkerSettings(
            worker_name=worker_name,
            consume_topic=consume_topic,
            produce_topic=produce_topic,
            consumer_group=worker_name,
        ),
        consumer=consumer,
        producer=producer,
        process=process,
    )


def _drain(runner: WorkerRunner) -> None:
    """Consume until the consumer idle-times out, then close."""
    try:
        runner.run_forever()
    finally:
        runner.consumer.close()
        runner.producer.close()


def _seed_enrich_input(store: ObjectStore, bootstrap: str, batch_id: str, source: str) -> str:
    """Write a hadith batch to MinIO and produce its pointer to dedup.done.

    Returns the object key. The enrich worker consumes ``pipeline.dedup.done``
    (its upstream is dedup), so the seed message is published there.
    """
    raw_key = f"dedup/{source}/{batch_id}/hadiths.parquet"
    table = build_hadith_table(
        [
            {
                "source_id": "h-1",
                "source_corpus": source,
                "collection_name": "bukhari",
                "matn_en": "Actions are judged by intentions",
                "grade": "sahih",
            },
        ]
    )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    store.put_object(raw_key, buf.getvalue())

    msg = PipelineMessage(batch_id=batch_id, source=source, b2_path=raw_key, record_count=1)
    producer = KafkaProducer(bootstrap_servers=bootstrap)
    producer.send(PIPELINE_DEDUP_DONE, serialize_message(msg))
    producer.flush()
    producer.close()
    return raw_key


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_enrich_normalize_ingest_e2e_through_kafka_topics(
    bootstrap: str,
    minio_store: ObjectStore,
    neo4j_container_module: Neo4jContainer,
) -> None:
    """A batch flows enrich → normalize → ingest across three real Kafka topics.

    Assert end state: the Hadith node MERGEd by the terminal ingest stage
    exists in Neo4j, proving the message traversed every topic and every
    stage's object hand-off worked against real infra.
    """
    from neo4j import GraphDatabase

    batch_id = "batch-kafka-e2e-001"
    source = "sunnah"
    _seed_enrich_input(minio_store, bootstrap, batch_id, source)

    driver = GraphDatabase.driver(
        neo4j_container_module.get_connection_url(),
        auth=("neo4j", NEO4J_TEST_PASSWORD),
    )

    # Stage 1: enrich consumes pipeline.dedup.done → produces pipeline.enrich.done
    _drain(
        _make_stage_runner(
            bootstrap=bootstrap,
            store=minio_store,
            worker_name="enrich-worker",
            consume_topic=PIPELINE_DEDUP_DONE,
            produce_topic=PIPELINE_ENRICH_DONE,
            process=EnrichProcessor(minio_store),
        )
    )

    # Stage 2: normalize consumes pipeline.enrich.done → produces pipeline.normalize.done
    _drain(
        _make_stage_runner(
            bootstrap=bootstrap,
            store=minio_store,
            worker_name="normalize-worker",
            consume_topic=PIPELINE_ENRICH_DONE,
            produce_topic=PIPELINE_NORMALIZE_DONE,
            process=NormalizeProcessor(minio_store),
        )
    )

    # Stage 3: ingest (terminal) consumes pipeline.normalize.done → MERGE into Neo4j
    _drain(
        _make_stage_runner(
            bootstrap=bootstrap,
            store=minio_store,
            worker_name="ingest-worker",
            consume_topic=PIPELINE_NORMALIZE_DONE,
            produce_topic=None,
            process=IngestProcessor(minio_store, neo4j_driver=driver),
        )
    )

    # End-to-end assertion: the hadith node landed in Neo4j.
    expected_hid = f"hdt:{source}:h-1"
    with driver.session() as session:
        record = session.run(
            "MATCH (n:Hadith {id: $id}) RETURN n.matn_en AS matn_en",
            id=expected_hid,
        ).single()
    driver.close()

    assert record is not None, (
        f"Hadith {expected_hid!r} not found in Neo4j — the message did not "
        "traverse the full enrich→normalize→ingest Kafka pipeline"
    )
    assert "Actions are judged by intentions" in record["matn_en"]


def test_kafka_topic_carries_pipeline_message_round_trip(bootstrap: str) -> None:
    """Sanity: a PipelineMessage survives a real Kafka publish→consume round-trip.

    Anchors the E2E test — proves the Kafka wire (serialize → produce →
    consume → parse) is intact independent of the worker processors, so an
    E2E failure is attributable to a stage, not the transport.
    """
    topic = "pipeline.roundtrip.test"
    msg = PipelineMessage(
        batch_id="batch-rt-001", source="sunnah", b2_path="raw/x.parquet", record_count=7
    )

    producer = KafkaProducer(bootstrap_servers=bootstrap)
    producer.send(topic, serialize_message(msg))
    producer.flush()
    producer.close()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id="roundtrip-test",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=CONSUMER_TIMEOUT_MS,
    )
    received = next(iter(consumer))
    consumer.close()

    parsed = json.loads(received.value)
    assert parsed["batch_id"] == "batch-rt-001"
    assert parsed["record_count"] == 7


def test_main_build_runner_wires_against_live_infra(
    bootstrap: str,
    minio_container: MinioContainer,
    neo4j_container_module: Neo4jContainer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The production ``main.build_runner`` entrypoints connect to live infra.

    Drives the *actual* env-driven wiring in each ``workers/<stage>/main.py``
    (not the test ``_make_stage_runner``). The ingest entrypoint runs
    ``driver.verify_connectivity()`` at build time, so a successful build is
    proof the env → Kafka/MinIO/Neo4j wiring resolves against real containers.
    """
    from workers.dedup import main as dedup_main
    from workers.enrich import main as enrich_main
    from workers.ingest import main as ingest_main
    from workers.normalize import main as normalize_main

    cfg = minio_container.get_config()
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", bootstrap)
    monkeypatch.setenv("PIPELINE_BUCKET", TEST_BUCKET)
    monkeypatch.setenv("S3_ENDPOINT_URL", f"http://{cfg['endpoint']}")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", cfg["access_key"])
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", cfg["secret_key"])
    monkeypatch.setenv("INGEST_CHECKPOINT_BACKEND", "in-memory")
    monkeypatch.setenv("NEO4J_URI", neo4j_container_module.get_connection_url())
    monkeypatch.setenv("NEO4J_USER", "neo4j")
    monkeypatch.setenv("NEO4J_PASSWORD", NEO4J_TEST_PASSWORD)

    # Non-terminal stages: build + tear down the real Kafka clients.
    for build in (dedup_main.build_runner, enrich_main.build_runner, normalize_main.build_runner):
        runner = build()
        runner.consumer.close()
        runner.producer.close()

    # Ingest: build_runner runs verify_connectivity() — a live Neo4j handshake.
    runner, driver = ingest_main.build_runner()
    try:
        assert runner.settings.worker_name == "ingest-worker"
    finally:
        runner.consumer.close()
        runner.producer.close()
        driver.close()
