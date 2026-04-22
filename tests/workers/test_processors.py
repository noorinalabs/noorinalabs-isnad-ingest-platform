"""Processor-level tests for each worker.

These exercise each processor end-to-end against the in-memory S3 fake
so we verify the read → transform → write → next-pointer flow without
any Kafka or network dependency.

Dedup and enrich processors have hard deps on the ``ml`` optional
dependency group (``faiss``, ``sentence-transformers``, ``transformers``).
Tests that exercise the ML code paths are gated on
``pytest.importorskip`` so the default ``uv sync`` suite still passes;
the ML-less paths rely on the processors' graceful-degradation
behaviour.
"""

from __future__ import annotations

import io
import json
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.parse.schemas import HADITH_SCHEMA
from src.resolve.schemas import PARALLEL_LINKS_SCHEMA
from tests.workers.conftest import FakeS3Client, build_hadith_parquet
from workers.dedup.processor import DedupProcessor
from workers.enrich.processor import HADITH_TOPICS_SCHEMA, EnrichProcessor
from workers.ingest.processor import IngestProcessor
from workers.lib.message import PipelineMessage
from workers.lib.object_store import ObjectStore
from workers.normalize.processor import NormalizeProcessor


def _seed(store: ObjectStore, key: str, payload: bytes) -> None:
    store.put_object(key, payload)


# --------------------------------------------------------------------------
# dedup
# --------------------------------------------------------------------------


class TestDedupProcessor:
    def test_writes_hadiths_and_empty_links_when_ml_absent(
        self,
        object_store: ObjectStore,
        sample_message: PipelineMessage,
        sample_hadith_parquet: bytes,
    ) -> None:
        _seed(object_store, sample_message.b2_path, sample_hadith_parquet)

        processor = DedupProcessor(object_store)
        # Force the ML-unavailable path regardless of installed deps.
        processor._ml_unavailable = True

        nxt = processor(sample_message)

        assert nxt.b2_path == "dedup/sunnah-api/batch-001/hadiths.parquet"
        assert object_store.get_object(nxt.b2_path) == sample_hadith_parquet

        links_bytes = object_store.get_object("dedup/sunnah-api/batch-001/parallel_links.parquet")
        links_table = pq.read_table(io.BytesIO(links_bytes))
        assert links_table.schema.equals(PARALLEL_LINKS_SCHEMA)
        assert links_table.num_rows == 0

    def test_empty_hadith_batch_emits_empty_links(
        self,
        object_store: ObjectStore,
        sample_message: PipelineMessage,
    ) -> None:
        _seed(object_store, sample_message.b2_path, build_hadith_parquet([]))

        processor = DedupProcessor(object_store)
        nxt = processor(sample_message)

        assert nxt.b2_path == "dedup/sunnah-api/batch-001/hadiths.parquet"
        links_bytes = object_store.get_object("dedup/sunnah-api/batch-001/parallel_links.parquet")
        links_table = pq.read_table(io.BytesIO(links_bytes))
        assert links_table.num_rows == 0

    def test_malformed_parquet_raises(
        self,
        object_store: ObjectStore,
        sample_message: PipelineMessage,
    ) -> None:
        _seed(object_store, sample_message.b2_path, b"not-a-parquet-file")

        processor = DedupProcessor(object_store)
        processor._ml_unavailable = False  # force ML path so Parquet parse runs

        # Even without ML, we still try to read the Parquet once ml deps
        # are claimed available — emulate that by monkey-patching _ensure_ml
        # to return True without actually loading a model.
        processor._ensure_ml = lambda: True  # type: ignore[method-assign]
        # And pre-populate _np so _embed won't crash if reached.
        import numpy as np  # noqa: PLC0415 — optional test-only import

        processor._np = np

        with pytest.raises((pa.ArrowInvalid, OSError)):
            processor(sample_message)

    @pytest.mark.ml
    def test_real_dedup_finds_near_duplicates(
        self,
        object_store: ObjectStore,
        sample_message: PipelineMessage,
        hadith_row,
    ) -> None:
        pytest.importorskip("faiss")
        pytest.importorskip("sentence_transformers")

        rows = [
            hadith_row(
                source_id="a",
                matn_en=(
                    "Actions are judged by their intentions and every person "
                    "shall have what he intended."
                ),
            ),
            hadith_row(
                source_id="b",
                matn_en=(
                    "Deeds are judged by intentions and every man will receive what he intended."
                ),
            ),
            hadith_row(
                source_id="c",
                source_corpus="thaqalayn",
                sect="shia",
                matn_en=(
                    "A completely different narration about the rituals of hajj "
                    "and the sacred months."
                ),
            ),
        ]
        _seed(object_store, sample_message.b2_path, build_hadith_parquet(rows))

        DedupProcessor(object_store, threshold=0.5)(sample_message)

        links_bytes = object_store.get_object("dedup/sunnah-api/batch-001/parallel_links.parquet")
        links = pq.read_table(io.BytesIO(links_bytes))
        assert links.schema.equals(PARALLEL_LINKS_SCHEMA)
        ids_a = links.column("hadith_id_a").to_pylist()
        ids_b = links.column("hadith_id_b").to_pylist()
        pairs = {tuple(sorted([a, b])) for a, b in zip(ids_a, ids_b)}
        assert ("a", "b") in pairs, f"expected near-duplicate pair, got {pairs}"


# --------------------------------------------------------------------------
# enrich
# --------------------------------------------------------------------------


class TestEnrichProcessor:
    def test_writes_hadiths_and_empty_topics_when_ml_absent(
        self,
        object_store: ObjectStore,
        sample_message: PipelineMessage,
        sample_hadith_parquet: bytes,
    ) -> None:
        _seed(object_store, sample_message.b2_path, sample_hadith_parquet)

        processor = EnrichProcessor(object_store)
        processor._ml_unavailable = True

        nxt = processor(sample_message)

        assert nxt.b2_path == "enriched/sunnah-api/batch-001/hadiths.parquet"
        assert object_store.get_object(nxt.b2_path) == sample_hadith_parquet

        topics_bytes = object_store.get_object(
            "enriched/sunnah-api/batch-001/hadith_topics.parquet"
        )
        topics = pq.read_table(io.BytesIO(topics_bytes))
        assert topics.schema.equals(HADITH_TOPICS_SCHEMA)
        assert topics.num_rows == 0

    def test_short_matn_rows_are_skipped_in_classification(
        self,
        object_store: ObjectStore,
        sample_message: PipelineMessage,
        hadith_row,
    ) -> None:
        rows = [
            hadith_row(source_id="a", matn_en="short"),
            hadith_row(source_id="b", matn_en=None),
        ]
        _seed(object_store, sample_message.b2_path, build_hadith_parquet(rows))

        processor = EnrichProcessor(object_store)

        class _StubClassifier:
            called_with: list[list[str]] = []

            def __call__(
                self, texts: list[str], *, candidate_labels: list[str], multi_label: bool
            ) -> list[dict[str, list]]:
                self.called_with.append(texts)
                return [{"labels": candidate_labels[:3], "scores": [0.5, 0.3, 0.2]} for _ in texts]

        processor._classifier = _StubClassifier()

        processor(sample_message)

        # No rows met the min length, classifier should not have been called.
        assert _StubClassifier.called_with == []
        topics = pq.read_table(
            io.BytesIO(
                object_store.get_object("enriched/sunnah-api/batch-001/hadith_topics.parquet")
            )
        )
        assert topics.num_rows == 0

    def test_stub_classifier_populates_topics_table(
        self,
        object_store: ObjectStore,
        sample_message: PipelineMessage,
        sample_hadith_parquet: bytes,
    ) -> None:
        _seed(object_store, sample_message.b2_path, sample_hadith_parquet)

        class _StubClassifier:
            def __call__(
                self, texts: list[str], *, candidate_labels: list[str], multi_label: bool
            ) -> list[dict[str, list]]:
                return [
                    {
                        "labels": ["theology", "ethics/conduct", "ritual/worship"],
                        "scores": [0.8, 0.15, 0.05],
                    }
                    for _ in texts
                ]

        processor = EnrichProcessor(object_store)
        processor._classifier = _StubClassifier()

        processor(sample_message)

        topics = pq.read_table(
            io.BytesIO(
                object_store.get_object("enriched/sunnah-api/batch-001/hadith_topics.parquet")
            )
        )
        assert topics.num_rows == 2
        assert topics.column("topic_1").to_pylist() == ["theology", "theology"]
        scores = topics.column("topic_1_score").to_pylist()
        assert len(scores) == 2
        assert all(abs(s - 0.8) < 1e-4 for s in scores)
        assert set(topics.column("source_id").to_pylist()) == {"sunnah:001", "sunnah:002"}


# --------------------------------------------------------------------------
# normalize
# --------------------------------------------------------------------------


class TestNormalizeProcessor:
    """Post-#192 D-ii: normalize fans out each hadith into per-label Parquets
    under a folder prefix + ``_MANIFEST.json`` written LAST."""

    @staticmethod
    def _read_manifest(store: ObjectStore, prefix: str) -> dict[str, Any]:
        body = store.get_object(f"{prefix}_MANIFEST.json")
        result: dict[str, Any] = json.loads(body.decode("utf-8"))
        return result

    @staticmethod
    def _read_nodes(store: ObjectStore, prefix: str, filename: str) -> list[dict[str, Any]]:
        body = store.get_object(f"{prefix}{filename}")
        table = pq.read_table(io.BytesIO(body))
        rows = table.to_pylist()
        # Decode the JSON-encoded props column for test assertions.
        return [{**r, "props": json.loads(r["props"])} for r in rows]

    def test_emits_folder_prefix_and_manifest(
        self,
        object_store: ObjectStore,
        sample_message: PipelineMessage,
        sample_hadith_parquet: bytes,
    ) -> None:
        _seed(object_store, sample_message.b2_path, sample_hadith_parquet)

        nxt = NormalizeProcessor(object_store)(sample_message)

        # b2_path is now a folder prefix with trailing slash (#192 D-ii).
        assert nxt.b2_path == "normalized/batch-001/"
        manifest = self._read_manifest(object_store, nxt.b2_path)
        assert manifest["batch_id"] == "batch-001"
        assert manifest["source"] == "sunnah-api"
        # total_row_count across every Parquet must equal nxt.record_count.
        assert manifest["total_row_count"] == nxt.record_count

    def test_fan_out_produces_expected_per_label_counts(
        self,
        object_store: ObjectStore,
        sample_message: PipelineMessage,
        hadith_row: Any,
    ) -> None:
        # Two hadiths sharing one collection, one with a full English isnad.
        rows = [
            hadith_row(
                source_id="h1",
                matn_en="text one",
                collection_name="bukhari",
            ),
            hadith_row(
                source_id="h2",
                matn_en="text two",
                collection_name="bukhari",
            ),
        ]
        rows[0]["isnad_raw_en"] = "Narrated Abu Hurayrah from Malik on the authority of Nafi"
        rows[1]["grade"] = "sahih"
        _seed(object_store, sample_message.b2_path, build_hadith_parquet(rows))

        nxt = NormalizeProcessor(object_store)(sample_message)

        manifest = self._read_manifest(object_store, nxt.b2_path)
        entries = {e["path"]: e for e in manifest["parquets"]}

        # Two hadith nodes (always).
        assert entries["hadiths.parquet"]["row_count"] == 2

        # One collection (deduped across the two hadiths).
        assert entries["collections.parquet"]["row_count"] == 1

        # Two chain nodes — one per hadith, regardless of isnad presence.
        assert entries["chains.parquet"]["row_count"] == 2

        # Narrators from the English isnad on h1. Three mentions expected
        # (Abu Hurayrah, Malik, Nafi) — the extractor splits on the
        # transmission keywords ``Narrated``/``from``/``on the authority of``.
        narrators = self._read_nodes(object_store, nxt.b2_path, "narrators.parquet")
        assert len(narrators) >= 2, f"expected multiple narrators extracted, got {narrators}"

        # Grading node only for h2 (the one with a grade set).
        gradings = self._read_nodes(object_store, nxt.b2_path, "gradings.parquet")
        assert len(gradings) == 1
        assert gradings[0]["props"]["grade"] == "sahih"

        # Edges present for APPEARS_IN (per-hadith) + GRADED_BY (h2) +
        # TRANSMITTED_TO (N-1 narrators in h1's chain) + NARRATED (first
        # narrator -> h1).
        edges_body = object_store.get_object(f"{nxt.b2_path}edges.parquet")
        edges_table = pq.read_table(io.BytesIO(edges_body))
        edge_labels = edges_table.column("label").to_pylist()
        assert edge_labels.count("APPEARS_IN") == 2
        assert edge_labels.count("GRADED_BY") == 1
        assert "NARRATED" in edge_labels
        assert "TRANSMITTED_TO" in edge_labels

    def test_every_node_row_matches_allowed_label_vocabulary(
        self,
        object_store: ObjectStore,
        sample_message: PipelineMessage,
        sample_hadith_parquet: bytes,
    ) -> None:
        from workers.lib.topics import ALLOWED_NODE_LABELS

        _seed(object_store, sample_message.b2_path, sample_hadith_parquet)
        nxt = NormalizeProcessor(object_store)(sample_message)

        manifest = self._read_manifest(object_store, nxt.b2_path)
        for entry in manifest["parquets"]:
            if entry["path"] == "edges.parquet":
                continue
            body = object_store.get_object(f"{nxt.b2_path}{entry['path']}")
            table = pq.read_table(io.BytesIO(body))
            for label in set(table.column("label").to_pylist()):
                assert label in ALLOWED_NODE_LABELS, f"normalize emitted unknown label {label!r}"

    def test_stable_narrator_ids_across_reruns(
        self,
        object_store: ObjectStore,
        fake_s3: FakeS3Client,
        sample_message: PipelineMessage,
        hadith_row: Any,
    ) -> None:
        row = hadith_row(source_id="h1", matn_en="t", collection_name="bukhari")
        row["isnad_raw_en"] = "Narrated Abu Hurayrah from Malik"
        _seed(object_store, sample_message.b2_path, build_hadith_parquet([row]))

        nxt1 = NormalizeProcessor(object_store)(sample_message)
        ids_run1 = sorted(
            n["id"] for n in self._read_nodes(object_store, nxt1.b2_path, "narrators.parquet")
        )

        # Wipe and re-run on identical input — every ID must match.
        fake_s3.objects = {
            k: v for k, v in fake_s3.objects.items() if not k[1].startswith("normalized/")
        }
        nxt2 = NormalizeProcessor(object_store)(sample_message)
        ids_run2 = sorted(
            n["id"] for n in self._read_nodes(object_store, nxt2.b2_path, "narrators.parquet")
        )

        assert ids_run1 == ids_run2
        assert all(i.startswith("nar:") for i in ids_run1)

    def test_manifest_row_counts_match_parquet_row_counts(
        self,
        object_store: ObjectStore,
        sample_message: PipelineMessage,
        sample_hadith_parquet: bytes,
    ) -> None:
        _seed(object_store, sample_message.b2_path, sample_hadith_parquet)
        nxt = NormalizeProcessor(object_store)(sample_message)

        manifest = self._read_manifest(object_store, nxt.b2_path)
        for entry in manifest["parquets"]:
            body = object_store.get_object(f"{nxt.b2_path}{entry['path']}")
            table = pq.read_table(io.BytesIO(body))
            assert table.num_rows == entry["row_count"], (
                f"manifest lies about {entry['path']}: "
                f"claims {entry['row_count']}, parquet has {table.num_rows}"
            )

    def test_no_part_files_remain_on_success(
        self,
        object_store: ObjectStore,
        fake_s3: FakeS3Client,
        sample_message: PipelineMessage,
        sample_hadith_parquet: bytes,
    ) -> None:
        _seed(object_store, sample_message.b2_path, sample_hadith_parquet)
        NormalizeProcessor(object_store)(sample_message)

        leaked = [k for (_bucket, k) in fake_s3.objects if ".part." in k]
        assert leaked == [], f"leaked .part staging files: {leaked}"

    def test_write_failure_leaves_no_manifest(
        self,
        object_store: ObjectStore,
        fake_s3: FakeS3Client,
        sample_message: PipelineMessage,
        sample_hadith_parquet: bytes,
    ) -> None:
        """If any per-label write fails, ingest must not see a manifest —
        no manifest means no ready signal, ingest skips the batch."""
        _seed(object_store, sample_message.b2_path, sample_hadith_parquet)

        processor = NormalizeProcessor(object_store)

        original_put = object_store.put_object
        call_count = {"n": 0}

        def flaky_put(
            key: str, data: bytes, content_type: str = "application/octet-stream"
        ) -> None:
            call_count["n"] += 1
            # Fail the 2nd .part write so at least one label lands but the
            # batch aborts before the manifest is written.
            if call_count["n"] == 2:
                raise RuntimeError("simulated B2 outage")
            original_put(key, data, content_type=content_type)

        object_store.put_object = flaky_put  # type: ignore[method-assign]

        with pytest.raises(RuntimeError, match="simulated B2 outage"):
            processor(sample_message)

        # Manifest must be absent.
        leaked_manifest = [k for (_bucket, k) in fake_s3.objects if k.endswith("_MANIFEST.json")]
        assert leaked_manifest == []

    def test_drops_rows_missing_required_fields(
        self,
        object_store: ObjectStore,
        sample_message: PipelineMessage,
        hadith_row: Any,
    ) -> None:
        """Relaxed-schema input with a null in a target-non-null column must
        be dropped before fan-out so we don't emit nodes for invalid rows."""
        good = hadith_row(source_id="good", matn_en="well-formed row")
        bad = hadith_row(source_id="bad")
        bad["source_corpus"] = None

        relaxed = pa.schema([pa.field(f.name, f.type, nullable=True) for f in HADITH_SCHEMA])
        by_col: dict[str, list[Any]] = {f.name: [] for f in relaxed}
        for r in (good, bad):
            for f in relaxed:
                by_col[f.name].append(r.get(f.name))
        table = pa.table(by_col, schema=relaxed)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        _seed(object_store, sample_message.b2_path, buf.getvalue())

        nxt = NormalizeProcessor(object_store)(sample_message)

        hadiths = self._read_nodes(object_store, nxt.b2_path, "hadiths.parquet")
        assert len(hadiths) == 1
        assert hadiths[0]["props"]["collection_name"] == "bukhari"

    def test_missing_required_column_raises(
        self,
        object_store: ObjectStore,
        sample_message: PipelineMessage,
    ) -> None:
        table = pa.table({"source_id": ["x"], "collection_name": ["y"], "sect": ["sunni"]})
        buf = io.BytesIO()
        pq.write_table(table, buf)
        _seed(object_store, sample_message.b2_path, buf.getvalue())

        with pytest.raises(ValueError, match="missing required columns"):
            NormalizeProcessor(object_store)(sample_message)


# --------------------------------------------------------------------------
# ingest — unchanged scope (owned by #13), but keep smoke test
# --------------------------------------------------------------------------


def test_ingest_returns_none_and_reads_input(
    object_store: ObjectStore,
    sample_message: PipelineMessage,
    sample_hadith_parquet: bytes,
) -> None:
    _seed(object_store, sample_message.b2_path, sample_hadith_parquet)

    result = IngestProcessor(object_store)(sample_message)

    assert result is None  # terminal stage


# --------------------------------------------------------------------------
# chain
# --------------------------------------------------------------------------


def test_processor_chain_propagates_batch_id(
    object_store: ObjectStore,
    sample_message: PipelineMessage,
    sample_hadith_parquet: bytes,
) -> None:
    """dedup → enrich → normalize must all carry the original batch_id and source."""
    _seed(object_store, sample_message.b2_path, sample_hadith_parquet)

    dedup = DedupProcessor(object_store)
    dedup._ml_unavailable = True
    enrich = EnrichProcessor(object_store)
    enrich._ml_unavailable = True
    normalize = NormalizeProcessor(object_store)

    after_dedup = dedup(sample_message)
    after_enrich = enrich(after_dedup)
    after_norm = normalize(after_enrich)

    assert after_norm.batch_id == "batch-001"
    assert after_norm.source == "sunnah-api"
    # Post-#192 D-ii: normalize emits a folder prefix, not a single object key.
    assert after_norm.b2_path == "normalized/batch-001/"


def test_dedup_failure_propagates_to_dlq_via_runner(
    object_store: ObjectStore,
    sample_message: PipelineMessage,
) -> None:
    """Runner wraps processor exceptions into a DLQ record — spot-check for dedup."""
    from tests.workers.conftest import FakeConsumer, FakeProducer
    from workers.lib.dlq import DLQ_TOPIC
    from workers.lib.message import serialize_message
    from workers.lib.runner import WorkerRunner, WorkerSettings

    object_store.put_object(sample_message.b2_path, b"not-a-parquet-file")

    processor = DedupProcessor(object_store)
    processor._ensure_ml = lambda: True  # type: ignore[method-assign]
    import numpy as np  # noqa: PLC0415

    processor._np = np

    producer = FakeProducer()
    runner = WorkerRunner(
        settings=WorkerSettings(
            worker_name="dedup-worker",
            consume_topic="pipeline.raw.new",
            produce_topic="pipeline.dedup.done",
            consumer_group="g",
        ),
        consumer=FakeConsumer([]),
        producer=producer,
        process=processor,
    )
    runner.handle_one(serialize_message(sample_message))

    assert len(producer.sent) == 1
    topic, value = producer.sent[0]
    assert topic == DLQ_TOPIC
    record = json.loads(value)
    assert record["original"]["batch_id"] == sample_message.batch_id
