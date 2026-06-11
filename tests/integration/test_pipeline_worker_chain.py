"""Pipeline worker-chain scenarios — dedup → enrich → normalize → graph-load.

noorinalabs-main#136 (follow-up to the #49 cross-repo suite). The per-worker
unit tests in ``tests/workers/`` already exercise each processor in isolation,
and ``test_real_data_flow`` covers the parse→graph-load tail. What was missing —
and what #136 asks for — is the **chained** worker pipeline: a batch flowing
through every stage the way the deployed Kafka workers run it, with each
stage's output becoming the next stage's input.

Two layers, mirroring Nikolaos's main#139 harness contract:

1. **In-process faithful-fake chain (default, no Docker).** Drives the REAL
   worker processors against the shared in-memory ``FakeS3Client`` / ``ObjectStore``
   from ``tests/workers/conftest.py``. Each processor is invoked exactly as the
   ``WorkerRunner`` invokes it — ``Processor(store)(msg) -> next_msg`` — and the
   next message's ``b2_path`` is fed to the next stage. Asserts the object-store
   contract at every hop (dedup prefix → enriched prefix → normalized manifest)
   so a stage that silently stops emitting its hand-off object fails here. This
   is the CI-durable deliverable: no Kafka, no S3, no Docker.

2. **Real-Neo4j graph-load tail (``@pytest.mark.integration``).** Runs the
   normalize output through ``IngestProcessor`` against a real Neo4j
   (testcontainers) and asserts the MERGEd nodes/edges land. Docker-gated by the
   ``integration`` marker; the live-execution-in-CI leg is tracked in
   noorinalabs-main#631 (Docker-Hub-reachability flake on shared runners).

The full Kafka-topic + MinIO live wiring (real brokers, real object store) is
the main#139 one-shot live run and the deployed stack — out of scope here;
this suite proves the worker *contract* composes, deterministically, in CI.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow.parquet as pq
import pytest

from tests.workers.conftest import FakeS3Client, build_hadith_parquet
from workers.dedup.processor import DedupProcessor
from workers.enrich.processor import EnrichProcessor
from workers.ingest.processor import IngestProcessor
from workers.lib.message import PipelineMessage
from workers.lib.object_store import ObjectStore
from workers.normalize.processor import NormalizeProcessor

# A small, well-formed batch covering two collections and two sects so the
# normalize fan-out emits Hadith + Collection (+ Narrator when isnad present)
# nodes and at least one edge — enough that the graph-load tail has something
# to MERGE. ML deps (dedup/enrich classifiers) are absent in CI, so those
# stages pass-through the hadith payload and emit empty links/topics; the
# chain still flows end-to-end, which is exactly the contract under test.
_ROWS: list[dict[str, Any]] = [
    {
        "source_id": "sunnah:bukhari:1",
        "source_corpus": "sunnah",
        "collection_name": "bukhari",
        "book_number": 1,
        "chapter_number": 1,
        "hadith_number": 1,
        "matn_ar": "إنما الأعمال بالنيات",
        "matn_en": (
            "Actions are judged by intentions, and every person will have only what they intended."
        ),
        "isnad_raw_ar": None,
        "isnad_raw_en": None,
        "full_text_ar": None,
        "full_text_en": None,
        "grade": "sahih",
        "chapter_name_ar": None,
        "chapter_name_en": "Revelation",
        "sect": "sunni",
    },
    {
        "source_id": "thaqalayn:kafi:1",
        "source_corpus": "thaqalayn",
        "collection_name": "al-kafi",
        "book_number": 1,
        "chapter_number": 1,
        "hadith_number": 1,
        "matn_ar": "العلم نور",
        "matn_en": "Knowledge is a light that Allah casts into the heart of whom He wills.",
        "isnad_raw_ar": None,
        "isnad_raw_en": None,
        "full_text_ar": None,
        "full_text_en": None,
        "grade": None,
        "chapter_name_ar": None,
        "chapter_name_en": "The Excellence of Knowledge",
        "sect": "shia",
    },
]


class _ResettableFakeS3(FakeS3Client):
    """``FakeS3Client`` plus the ``list_objects_v2`` / ``delete_objects`` surface
    the ``PipelineResetter`` uses, so a SINGLE in-memory store backs both the
    worker chain (get/put/copy) and the admin stage-reset. Mirrors the list/
    delete semantics of ``tests/test_pipeline/test_reset.py``'s fake."""

    def list_objects_v2(
        self, *, Bucket: str, Prefix: str, ContinuationToken: str | None = None
    ) -> dict[str, Any]:
        keys = sorted(k for (b, k) in self.objects if b == Bucket and k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}

    def delete_objects(self, *, Bucket: str, Delete: dict[str, Any]) -> dict[str, Any]:
        for obj in Delete["Objects"]:
            self.objects.pop((Bucket, obj["Key"]), None)
        return {"Deleted": Delete["Objects"]}


def _seed_raw(store: ObjectStore, msg: PipelineMessage, payload: bytes) -> None:
    store.put_object(msg.b2_path, payload)


def _keys_under(store: ObjectStore, prefix: str) -> list[str]:
    """Object keys currently present under ``prefix`` (test introspection)."""
    client: Any = store.client
    return [k for (b, k) in client.objects if b == store.bucket and k.startswith(prefix)]


def _read_table(store: ObjectStore, key: str) -> Any:
    with store.get_object(key) as stream:
        return pq.read_table(stream)


def _run_chain_to_normalize(store: ObjectStore, raw_msg: PipelineMessage) -> PipelineMessage:
    """Drive dedup → enrich → normalize, returning the normalize-stage message.

    Each hop is invoked exactly as ``WorkerRunner`` does: the processor is
    called with the current message and returns the next-stage message whose
    ``b2_path`` points at the object the next stage must consume.
    """
    dedup_msg = DedupProcessor(store)(raw_msg)
    enrich_msg = EnrichProcessor(store)(dedup_msg)
    normalize_msg = NormalizeProcessor(store)(enrich_msg)
    return normalize_msg


class TestInProcessWorkerChain:
    """Layer 1 — faithful-fake chain, no Docker. Runs everywhere."""

    @pytest.fixture
    def store(self) -> ObjectStore:
        return ObjectStore(bucket="test-bucket", client=FakeS3Client())

    @pytest.fixture
    def raw_msg(self) -> PipelineMessage:
        return PipelineMessage(
            batch_id="chain-batch-001",
            source="sunnah-api",
            b2_path="raw/sunnah-api/2026-06-10/hadiths.parquet",
            record_count=len(_ROWS),
        )

    def test_dedup_hands_off_to_enriched_prefix(
        self, store: ObjectStore, raw_msg: PipelineMessage
    ) -> None:
        """dedup pass-through lands the hadith payload at the dedup prefix and
        names it as the next stage's input."""
        _seed_raw(store, raw_msg, build_hadith_parquet(_ROWS))
        dedup_msg = DedupProcessor(store)(raw_msg)
        assert dedup_msg.b2_path == "dedup/sunnah-api/chain-batch-001/hadiths.parquet"
        # The hand-off object exists and preserves the row set.
        table = _read_table(store, dedup_msg.b2_path)
        assert table.num_rows == len(_ROWS)
        # parallel_links side-output is always written (empty without ML).
        with store.get_object("dedup/sunnah-api/chain-batch-001/parallel_links.parquet") as s:
            assert pq.read_table(s) is not None

    def test_enrich_hands_off_to_normalize(
        self, store: ObjectStore, raw_msg: PipelineMessage
    ) -> None:
        _seed_raw(store, raw_msg, build_hadith_parquet(_ROWS))
        dedup_msg = DedupProcessor(store)(raw_msg)
        enrich_msg = EnrichProcessor(store)(dedup_msg)
        assert enrich_msg.b2_path == "enriched/sunnah-api/chain-batch-001/hadiths.parquet"
        table = _read_table(store, enrich_msg.b2_path)
        assert table.num_rows == len(_ROWS)

    def test_full_chain_produces_normalize_manifest(
        self, store: ObjectStore, raw_msg: PipelineMessage
    ) -> None:
        """The whole dedup→enrich→normalize chain produces the normalized
        manifest (ingest's ready signal) plus per-label node Parquets and the
        edges Parquet."""
        _seed_raw(store, raw_msg, build_hadith_parquet(_ROWS))
        normalize_msg = _run_chain_to_normalize(store, raw_msg)

        # Normalize emits a folder-prefix message (trailing slash) the ingest
        # stage treats as a manifest-gated ready signal. The manifest file is
        # written LAST so its presence implies every Parquet is durable.
        assert normalize_msg.b2_path == "normalized/chain-batch-001/"
        with store.get_object("normalized/chain-batch-001/_MANIFEST.json") as s:
            manifest = json.loads(s.read().decode("utf-8"))
        parquets = manifest["parquets"]
        labels = {e.get("label") for e in parquets if e.get("label")}
        # Two collections + two hadiths fan out to at least Hadith + Collection.
        assert "Hadith" in labels
        assert "Collection" in labels
        # Every node Parquet named in the manifest actually exists.
        for entry in parquets:
            with store.get_object(f"normalized/chain-batch-001/{entry['path']}") as s:
                assert pq.read_table(s) is not None

    def test_chain_is_idempotent_object_store_state(
        self, store: ObjectStore, raw_msg: PipelineMessage
    ) -> None:
        """Re-running the chain over the same batch overwrites in place (no
        duplicate-prefix divergence) — the stage outputs are content-stable.

        The manifest carries a ``created_at`` timestamp that legitimately
        changes per run, so we compare the structural payload (the per-label
        Parquet entries + row counts), not raw bytes."""

        def _manifest_shape(store: ObjectStore) -> Any:
            with store.get_object("normalized/chain-batch-001/_MANIFEST.json") as s:
                m = json.loads(s.read().decode("utf-8"))
            # The edges entry has no `label` (None); coerce to "" so the tuple
            # sort is total and the shape is comparable across runs.
            return sorted(
                (e.get("label") or "", e["path"], e["row_count"]) for e in m["parquets"]
            ), m["total_row_count"]

        _seed_raw(store, raw_msg, build_hadith_parquet(_ROWS))
        first = _run_chain_to_normalize(store, raw_msg)
        shape_a = _manifest_shape(store)
        second = _run_chain_to_normalize(store, raw_msg)
        shape_b = _manifest_shape(store)
        assert first.b2_path == second.b2_path
        assert shape_a == shape_b

    def test_ingest_without_driver_validates_manifest_then_skips(
        self, store: ObjectStore, raw_msg: PipelineMessage
    ) -> None:
        """The terminal ingest stage, given no Neo4j driver, still reads and
        validates the normalize manifest + every referenced Parquet (the
        read-path the unit suite relies on) and returns cleanly. This proves
        the normalize→ingest hand-off contract without a database."""
        _seed_raw(store, raw_msg, build_hadith_parquet(_ROWS))
        normalize_msg = _run_chain_to_normalize(store, raw_msg)
        # neo4j_driver=None → validates manifest + parquets, skips the write.
        result = IngestProcessor(store, neo4j_driver=None)(normalize_msg)
        assert result is None


class TestAdminStageResetThenRerun:
    """Layer 1 (no Docker) — the #108 admin-reset workflow: reset a stage's B2
    prefix, then confirm a subsequent rerun of the chain repopulates it.

    This is the #136 acceptance bullet "Admin CLI resets a given pipeline stage
    and subsequent reruns succeed", exercised as a workflow that combines the
    real ``PipelineResetter`` with the real worker chain over one shared store.
    Reset *internals* (paging, Kafka offset reset, audit) are covered by
    ``tests/test_pipeline/test_reset.py``; this proves the reset→rerun loop.
    """

    @pytest.fixture
    def store(self) -> ObjectStore:
        return ObjectStore(bucket="test-bucket", client=_ResettableFakeS3())

    @pytest.fixture
    def raw_msg(self) -> PipelineMessage:
        return PipelineMessage(
            batch_id="reset-batch-001",
            source="sunnah-api",
            b2_path="raw/sunnah-api/2026-06-10/hadiths.parquet",
            record_count=len(_ROWS),
        )

    def test_stage_reset_clears_prefix_then_rerun_repopulates(
        self, store: ObjectStore, raw_msg: PipelineMessage
    ) -> None:
        from pathlib import Path
        from tempfile import TemporaryDirectory

        from src.pipeline.reset import PipelineResetter, ResetScope

        # --- first run: chain populates the dedup prefix ---
        _seed_raw(store, raw_msg, build_hadith_parquet(_ROWS))
        DedupProcessor(store)(raw_msg)
        assert _keys_under(store, "dedup/"), "dedup prefix should be populated after run 1"

        # --- admin resets the dedup stage ---
        class _NoopKafka:
            def reset_consumer_offsets(self, topic: str, group_id: str) -> None: ...
            def delete_topic_data(self, topic: str) -> None: ...

        class _NoopNeo4j:
            def truncate_hadith_graph(self) -> int:
                return 0

        class _NoopPg:
            def truncate_hadith_metadata(self) -> int:
                return 0

        with TemporaryDirectory() as data_dir:
            resetter = PipelineResetter(
                object_store=store,
                kafka_admin=_NoopKafka(),
                neo4j=_NoopNeo4j(),
                pg=_NoopPg(),
                data_dir=Path(data_dir),
            )
            report, _entry, _path = resetter.reset(ResetScope.stage_scope("dedup"))

        assert report.s3_objects_deleted >= 1
        assert not _keys_under(store, "dedup/"), "dedup prefix must be empty after reset"
        # The raw input is untouched by a dedup-stage reset, so rerun is possible.
        assert _keys_under(store, "raw/"), "raw input must survive a dedup-stage reset"

        # --- subsequent rerun succeeds and repopulates the dedup prefix ---
        rerun_msg = DedupProcessor(store)(raw_msg)
        assert rerun_msg.b2_path == "dedup/sunnah-api/reset-batch-001/hadiths.parquet"
        assert _keys_under(store, "dedup/"), "rerun after reset must repopulate dedup prefix"


@pytest.mark.integration
class TestGraphLoadTailRealNeo4j:
    """Layer 2 — graph-load tail against a real Neo4j (testcontainers).

    Docker-gated by the ``integration`` marker. The normalize output is MERGEd
    by the real ``IngestProcessor`` and the resulting graph is queried back.
    Live-in-CI execution tracked in noorinalabs-main#631.
    """

    def test_chain_then_graph_load_creates_nodes(self, neo4j_container) -> None:
        from neo4j import GraphDatabase

        store = ObjectStore(bucket="test-bucket", client=FakeS3Client())
        raw_msg = PipelineMessage(
            batch_id="chain-batch-neo4j",
            source="sunnah-api",
            b2_path="raw/sunnah-api/2026-06-10/hadiths.parquet",
            record_count=len(_ROWS),
        )
        store.put_object(raw_msg.b2_path, build_hadith_parquet(_ROWS))
        normalize_msg = _run_chain_to_normalize(store, raw_msg)

        driver = GraphDatabase.driver(
            neo4j_container.get_connection_url(),
            auth=("neo4j", "testpassword123"),
        )

        def _scalar(session: Any, cypher: str) -> int:
            record = session.run(cypher).single()
            assert record is not None
            return int(record["c"])

        try:
            # Clean slate so repeated runs are deterministic.
            with driver.session() as session:
                session.run("MATCH (n) DETACH DELETE n")
            IngestProcessor(store, neo4j_driver=driver)(normalize_msg)
            with driver.session() as session:
                hadith_count = _scalar(session, "MATCH (h:Hadith) RETURN count(h) AS c")
                coll_count = _scalar(session, "MATCH (c:Collection) RETURN count(c) AS c")
        finally:
            driver.close()

        # Two hadiths across two collections were fanned out and MERGEd.
        assert hadith_count == 2
        assert coll_count == 2
