"""Full four-stage worker-chain E2E through real Kafka + MinIO + Neo4j (main#136).

What this adds over the existing integration suite
--------------------------------------------------
* ``test_kafka_worker_e2e.py`` (#55) drives **enrich → normalize → ingest**
  through real Kafka, but *seeds at* ``pipeline.dedup.done`` — the dedup
  worker itself never runs, and its raw → ``dedup/.../hadiths.parquet``
  object hand-off is not exercised over real infra.
* ``test_e2e_inprocess.py`` (ig#62 / main#139) runs the whole chain but
  **in-process** — an in-memory broker and an emulator Neo4j sink — and
  asserts node *counts*, not the exact node identities.
* ``test_minio_dedup_object_store.py`` (#55) exercises the MinIO object-store
  contract through ``DedupProcessor`` but with no Kafka chain.

This module closes the remaining gap: the **complete**
``dedup → enrich → normalize → ingest`` chain across FOUR real Kafka topics
and a shared real MinIO bucket, terminating in a real Neo4j MERGE — with the
dedup worker as the first hop consuming ``pipeline.raw.landed``. It also
pins two contracts that the existing suite leaves unguarded end to end:

1. **Hadith-id parity (#63).** The fixture's ``source_id`` is corpus-prefixed
   exactly as the parsers emit it (``sunnah:bukhari:1:1``). The terminal
   Neo4j node id must equal the batch loader's ``hdt:{source_id}`` — NOT the
   doubled ``hdt:sunnah:sunnah:bukhari:1:1``. The #55 E2E uses a bare
   ``source_id="h-1"`` that cannot surface the doubling; this one can.
2. **Admin stage-reset (#108).** After the chain lands, a ``stage`` reset of
   the ``normalized`` prefix wipes only that stage's MinIO objects (the raw
   and dedup prefixes survive), proving the reset scope is honoured against a
   real object store rather than the ``FakeS3Client`` the unit suite uses.

Determinism note
----------------
Like the #55 E2E, each ``WorkerRunner`` here is the production
``WorkerRunner`` + production processor, but built with a test-only consumer
config (``auto_offset_reset="earliest"`` + a ``consumer_timeout_ms`` so
``run_forever`` drains and returns instead of blocking like a long-lived
worker). The topic topology and object hand-offs are unchanged.

ML deps are NOT required: dedup and enrich both degrade gracefully (empty
links / empty topics side-table + pass-through copy) without
``sentence-transformers`` / ``faiss`` / ``transformers``, so the hadith
payload reaches normalize and ingest on every hop.

Runtime requirement
-------------------
``@pytest.mark.integration`` — needs Docker for THREE containers (Kafka,
MinIO, Neo4j). Excluded from the default ``pytest -m "not integration"`` run;
runs via ``make test-integration``. ``importorskip`` on
``testcontainers.kafka`` / ``testcontainers.minio`` / ``boto3`` / ``kafka``
keeps a Docker-less lane collecting cleanly.
"""

from __future__ import annotations

import io
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
from kafka import KafkaProducer  # noqa: E402  # type: ignore[import-untyped]
from testcontainers.kafka import KafkaContainer  # noqa: E402
from testcontainers.minio import MinioContainer  # noqa: E402
from testcontainers.neo4j import Neo4jContainer  # noqa: E402

from tests.factories import build_hadith_table  # noqa: E402
from workers.dedup.processor import DedupProcessor  # noqa: E402
from workers.enrich.processor import EnrichProcessor  # noqa: E402
from workers.ingest.processor import IngestProcessor  # noqa: E402
from workers.lib.message import PipelineMessage, serialize_message  # noqa: E402
from workers.lib.object_store import ObjectStore  # noqa: E402
from workers.lib.runner import WorkerRunner, WorkerSettings  # noqa: E402
from workers.lib.topics import (  # noqa: E402
    PIPELINE_DEDUP_DONE,
    PIPELINE_ENRICH_DONE,
    PIPELINE_NORMALIZE_DONE,
    PIPELINE_RAW_LANDED,
)
from workers.normalize.processor import NormalizeProcessor  # noqa: E402

pytestmark = pytest.mark.integration

TEST_BUCKET = "noorinalabs-pipeline"
NEO4J_TEST_PASSWORD = "testpassword123"
CONSUMER_TIMEOUT_MS = 20_000

# A real, corpus-prefixed source_id — exactly the shape the parsers emit via
# ``generate_source_id`` (``<corpus>:<collection>:<parts>``). This is what makes
# the #63 doubling observable: a bare id like "h-1" cannot.
SOURCE = "sunnah"
SOURCE_ID = "sunnah:bukhari:1:1"
# The id the batch loader (``src/graph/load_nodes.py``: ``hdt:{sid}``) MERGEs.
EXPECTED_HADITH_ID = f"hdt:{SOURCE_ID}"


# ---------------------------------------------------------------------------
# Container fixtures (module-scoped so all tests share one stack)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def kafka_container() -> Iterator[KafkaContainer]:
    with KafkaContainer() as kafka:
        yield kafka


@pytest.fixture(scope="module")
def minio_container() -> Iterator[MinioContainer]:
    with MinioContainer(access_key="minio", secret_key="minio12345") as minio:
        yield minio


@pytest.fixture(scope="module")
def neo4j_container_module() -> Iterator[Neo4jContainer]:
    with Neo4jContainer("neo4j:5-community", password=NEO4J_TEST_PASSWORD) as neo4j:
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
    worker_name: str,
    consume_topic: str,
    produce_topic: str | None,
    process: Any,
) -> WorkerRunner:
    """Production ``WorkerRunner`` + processor, bounded for a test drain.

    Mirrors ``workers.<stage>.main.build_runner`` but with
    ``auto_offset_reset="earliest"`` and a ``consumer_timeout_ms`` so
    ``run_forever`` returns once the topic drains instead of blocking.
    """
    from kafka import KafkaConsumer  # type: ignore[import-untyped]

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
    try:
        runner.run_forever()
    finally:
        runner.consumer.close()
        runner.producer.close()


def _seed_raw_input(store: ObjectStore, bootstrap: str, batch_id: str) -> str:
    """Write a raw hadith batch to MinIO and produce its pointer to raw.landed.

    The dedup worker is the first hop, so the seed message is published to
    ``pipeline.raw.landed`` (dedup's upstream). ``source_id`` is corpus-prefixed
    so the #63 doubling is observable end to end.
    """
    raw_key = f"raw/{SOURCE}/{batch_id}/hadiths.parquet"
    table = build_hadith_table(
        [
            {
                "source_id": SOURCE_ID,
                "source_corpus": SOURCE,
                "collection_name": "bukhari",
                "matn_en": "Actions are judged by intentions",
                "grade": "sahih",
            },
        ]
    )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    store.put_object(raw_key, buf.getvalue())

    msg = PipelineMessage(batch_id=batch_id, source=SOURCE, b2_path=raw_key, record_count=1)
    producer = KafkaProducer(bootstrap_servers=bootstrap)
    producer.send(PIPELINE_RAW_LANDED, serialize_message(msg))
    producer.flush()
    producer.close()
    return raw_key


def _run_full_chain(bootstrap: str, store: ObjectStore, neo4j: Neo4jContainer) -> None:
    """Drive dedup → enrich → normalize → ingest across four real Kafka topics."""
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(neo4j.get_connection_url(), auth=("neo4j", NEO4J_TEST_PASSWORD))
    try:
        # Hop 1: dedup consumes pipeline.raw.landed → produces pipeline.dedup.done
        _drain(
            _make_stage_runner(
                bootstrap=bootstrap,
                worker_name="dedup-worker",
                consume_topic=PIPELINE_RAW_LANDED,
                produce_topic=PIPELINE_DEDUP_DONE,
                process=DedupProcessor(store),
            )
        )
        # Hop 2: enrich consumes pipeline.dedup.done → produces pipeline.enrich.done
        _drain(
            _make_stage_runner(
                bootstrap=bootstrap,
                worker_name="enrich-worker",
                consume_topic=PIPELINE_DEDUP_DONE,
                produce_topic=PIPELINE_ENRICH_DONE,
                process=EnrichProcessor(store),
            )
        )
        # Hop 3: normalize consumes pipeline.enrich.done → produces pipeline.normalize.done
        _drain(
            _make_stage_runner(
                bootstrap=bootstrap,
                worker_name="normalize-worker",
                consume_topic=PIPELINE_ENRICH_DONE,
                produce_topic=PIPELINE_NORMALIZE_DONE,
                process=NormalizeProcessor(store),
            )
        )
        # Hop 4: ingest (terminal) consumes pipeline.normalize.done → MERGE into Neo4j
        _drain(
            _make_stage_runner(
                bootstrap=bootstrap,
                worker_name="ingest-worker",
                consume_topic=PIPELINE_NORMALIZE_DONE,
                produce_topic=None,
                process=IngestProcessor(store, neo4j_driver=driver),
            )
        )
    finally:
        driver.close()


def _list_keys(store: ObjectStore, prefix: str) -> list[str]:
    resp = store.client.list_objects_v2(Bucket=store.bucket, Prefix=prefix)
    return [obj["Key"] for obj in resp.get("Contents", []) or []]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_full_dedup_to_ingest_chain_lands_hadith_in_neo4j(
    bootstrap: str,
    minio_store: ObjectStore,
    neo4j_container_module: Neo4jContainer,
) -> None:
    """A raw batch traverses dedup → enrich → normalize → ingest across four
    real Kafka topics and the Hadith node lands in Neo4j with the canonical id.
    """
    from neo4j import GraphDatabase

    batch_id = "batch-chain-e2e-001"
    _seed_raw_input(minio_store, bootstrap, batch_id)
    _run_full_chain(bootstrap, minio_store, neo4j_container_module)

    driver = GraphDatabase.driver(
        neo4j_container_module.get_connection_url(),
        auth=("neo4j", NEO4J_TEST_PASSWORD),
    )
    try:
        with driver.session() as session:
            record = session.run(
                "MATCH (n:Hadith {id: $id}) RETURN n.matn_en AS matn_en",
                id=EXPECTED_HADITH_ID,
            ).single()
            # Proof there is exactly ONE Hadith node — no doubled-id sibling.
            count = session.run("MATCH (n:Hadith) RETURN count(n) AS c").single()["c"]
    finally:
        driver.close()

    assert record is not None, (
        f"Hadith {EXPECTED_HADITH_ID!r} not found — the message did not traverse "
        "the full dedup→enrich→normalize→ingest Kafka chain"
    )
    assert "Actions are judged by intentions" in record["matn_en"]
    assert count == 1, f"expected exactly one Hadith node, found {count}"


def test_streaming_hadith_id_matches_batch_loader_through_real_neo4j(
    bootstrap: str,
    minio_store: ObjectStore,
    neo4j_container_module: Neo4jContainer,
) -> None:
    """End-to-end #63 guard: the id the chain MERGEs equals the batch loader's.

    The fixture ``source_id`` is corpus-prefixed (``sunnah:bukhari:1:1``).
    The terminal Neo4j node id must be ``hdt:sunnah:bukhari:1:1`` — the batch
    loader's ``hdt:{source_id}`` — and must NOT carry a doubled corpus
    (``hdt:sunnah:sunnah:bukhari:1:1``). This is the live-infra counterpart to
    the normalize unit guard, proving the contract holds all the way into the
    graph, not just at the normalize Parquet.
    """
    from neo4j import GraphDatabase

    batch_id = "batch-id-parity-001"
    _seed_raw_input(minio_store, bootstrap, batch_id)
    _run_full_chain(bootstrap, minio_store, neo4j_container_module)

    driver = GraphDatabase.driver(
        neo4j_container_module.get_connection_url(),
        auth=("neo4j", NEO4J_TEST_PASSWORD),
    )
    try:
        with driver.session() as session:
            ids = [r["id"] for r in session.run("MATCH (n:Hadith) RETURN n.id AS id")]
    finally:
        driver.close()

    assert ids == [EXPECTED_HADITH_ID], (
        f"Hadith ids in graph {ids!r} != expected [{EXPECTED_HADITH_ID!r}] — "
        "streaming and batch must MERGE the same node (#63)"
    )
    assert all("sunnah:sunnah" not in i for i in ids), "corpus prefix doubled (#63)"


@pytest.mark.xfail(
    strict=True,
    reason=(
        "BUG (ig#TBD, reported to team lead): src/pipeline/reset._delete_prefix "
        "calls client.delete_objects() which a real S3-compatible store (MinIO, "
        "and B2) rejects with 'MissingContentMD5' under botocore>=1.36 — the bulk "
        "DeleteObjects op needs a Content-MD5/checksum header boto3 no longer adds "
        "by default. The unit suite never caught it because FakeS3Client.delete_"
        "objects is an in-memory no-op. This integration scenario is the regression "
        "guard; remove the xfail once reset uses per-object delete_object (which "
        "ObjectStore already does) or sets the checksum header."
    ),
)
def test_admin_stage_reset_wipes_only_normalized_prefix_on_real_minio(
    bootstrap: str,
    minio_store: ObjectStore,
    neo4j_container_module: Neo4jContainer,
) -> None:
    """Admin ``stage`` reset (#108) honours scope against a real MinIO store.

    After the chain lands objects under raw/, dedup/, and normalized/, a
    ``stage`` reset of the ``normalized`` prefix must delete only that prefix's
    objects — the upstream raw/ and dedup/ payloads survive, so the stage can be
    re-run without re-acquiring or re-deduping. The unit suite proves the reset
    *logic* against ``FakeS3Client``; this proves it against the real
    ``list_objects_v2`` / ``delete_objects`` S3 surface the resetter calls — and
    currently surfaces a real defect in that surface (see the ``xfail`` reason).
    """
    from src.pipeline.reset import (
        STAGE_PREFIXES,
        PipelineResetter,
        ResetScope,
    )

    batch_id = "batch-reset-e2e-001"
    _seed_raw_input(minio_store, bootstrap, batch_id)
    _run_full_chain(bootstrap, minio_store, neo4j_container_module)

    # Pre-condition: all three upstream prefixes carry objects.
    assert _list_keys(minio_store, "raw/"), "raw prefix should be populated"
    assert _list_keys(minio_store, "dedup/"), "dedup prefix should be populated"
    normalized_before = _list_keys(minio_store, STAGE_PREFIXES["normalized"])
    assert normalized_before, "normalized prefix should be populated by the chain"

    # A stage reset only touches MinIO + Kafka offsets; the Kafka/Neo4j/PG
    # adapters are not exercised by a ``stage`` scope, so simple no-op stand-ins
    # keep the test focused on the real object-store deletion path.
    class _NoOpKafkaAdmin:
        def reset_consumer_offsets(self, topic: str, group_id: str) -> None:
            pass

        def delete_topic_data(self, topic: str) -> None:
            pass

    class _NoOpNeo4j:
        def truncate_hadith_graph(self) -> int:
            return 0

    class _NoOpPg:
        def truncate_hadith_metadata(self) -> int:
            return 0

    resetter = PipelineResetter(
        object_store=minio_store,
        kafka_admin=_NoOpKafkaAdmin(),
        neo4j=_NoOpNeo4j(),
        pg=_NoOpPg(),
        data_dir=_tmp_audit_dir(),
    )
    report, _entry, _path = resetter.reset(ResetScope.stage_scope("normalized"))

    # The normalized prefix is now empty; raw/ and dedup/ are untouched.
    assert _list_keys(minio_store, STAGE_PREFIXES["normalized"]) == []
    assert _list_keys(minio_store, "raw/"), "raw prefix must survive a normalized-stage reset"
    assert _list_keys(minio_store, "dedup/"), "dedup prefix must survive a normalized-stage reset"
    assert report.s3_objects_deleted == len(normalized_before)
    assert report.s3_prefixes_deleted == [STAGE_PREFIXES["normalized"]]


def _tmp_audit_dir() -> Any:
    """A throwaway audit dir for the reset's audit write (real filesystem)."""
    import tempfile
    from pathlib import Path

    return Path(tempfile.mkdtemp(prefix="reset-audit-"))
