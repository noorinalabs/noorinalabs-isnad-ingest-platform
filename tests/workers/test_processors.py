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

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.parse.schemas import HADITH_SCHEMA
from src.resolve.schemas import PARALLEL_LINKS_SCHEMA
from tests.workers.conftest import build_hadith_parquet
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
    def test_pass_through_when_schema_already_matches(
        self,
        object_store: ObjectStore,
        sample_message: PipelineMessage,
        sample_hadith_parquet: bytes,
    ) -> None:
        _seed(object_store, sample_message.b2_path, sample_hadith_parquet)

        nxt = NormalizeProcessor(object_store)(sample_message)

        assert nxt.b2_path == "normalized/batch-001/hadiths.parquet"
        assert nxt.record_count == 2
        out = pq.read_table(io.BytesIO(object_store.get_object(nxt.b2_path)))
        assert out.schema.equals(HADITH_SCHEMA)
        assert out.num_rows == 2

    def test_drops_rows_missing_required_fields(
        self,
        object_store: ObjectStore,
        sample_message: PipelineMessage,
        hadith_row,
    ) -> None:
        """Input arrives with a relaxed (fully-nullable) schema containing a
        null in a target-non-nullable column; normalize must drop that row
        before casting to the strict target schema."""
        good = hadith_row(source_id="good", matn_en="well-formed row")
        bad = hadith_row(source_id="bad")
        bad["source_corpus"] = None  # null in a column that target schema marks non-null

        # Build a relaxed schema where every field is nullable so pa/parquet
        # accepts the null.
        relaxed = pa.schema([pa.field(f.name, f.type, nullable=True) for f in HADITH_SCHEMA])
        by_col: dict[str, list] = {f.name: [] for f in relaxed}
        for row in (good, bad):
            for f in relaxed:
                by_col[f.name].append(row.get(f.name))
        table = pa.table(by_col, schema=relaxed)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        _seed(object_store, sample_message.b2_path, buf.getvalue())

        nxt = NormalizeProcessor(object_store)(sample_message)

        out = pq.read_table(io.BytesIO(object_store.get_object(nxt.b2_path)))
        assert out.num_rows == 1
        assert out.column("source_id").to_pylist() == ["good"]
        assert nxt.record_count == 1

    def test_strips_unexpected_extra_columns(
        self,
        object_store: ObjectStore,
        sample_message: PipelineMessage,
        hadith_row,
    ) -> None:
        row = hadith_row(source_id="x", matn_en="text")
        cols = {f.name: [row[f.name]] for f in HADITH_SCHEMA}
        cols["unexpected_extra"] = ["ignore-me"]
        table = pa.table(cols)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        _seed(object_store, sample_message.b2_path, buf.getvalue())

        nxt = NormalizeProcessor(object_store)(sample_message)

        out = pq.read_table(io.BytesIO(object_store.get_object(nxt.b2_path)))
        assert out.schema.equals(HADITH_SCHEMA)
        assert "unexpected_extra" not in out.column_names

    def test_missing_required_column_raises(
        self,
        object_store: ObjectStore,
        sample_message: PipelineMessage,
    ) -> None:
        # Build a Parquet that omits the required ``source_corpus`` column.
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
    assert after_norm.b2_path == "normalized/batch-001/hadiths.parquet"


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
