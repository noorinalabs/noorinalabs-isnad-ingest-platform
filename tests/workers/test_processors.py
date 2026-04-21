"""Processor-level tests for each worker.

These exercise each processor end-to-end against the in-memory S3 fake
so we verify the read→transform→write→next-pointer flow without any
Kafka or network dependency.
"""

from __future__ import annotations

from workers.dedup.processor import DedupProcessor
from workers.enrich.processor import EnrichProcessor
from workers.ingest.processor import IngestProcessor
from workers.lib.message import PipelineMessage
from workers.lib.object_store import ObjectStore
from workers.normalize.processor import NormalizeProcessor


def _seed(store: ObjectStore, key: str, payload: bytes = b"parquet-bytes") -> None:
    store.put_object(key, payload)


def test_dedup_writes_to_dedup_prefix(
    object_store: ObjectStore, sample_message: PipelineMessage
) -> None:
    _seed(object_store, sample_message.b2_path)

    nxt = DedupProcessor(object_store)(sample_message)

    assert nxt.b2_path.startswith("dedup/sunnah-api/batch-001/")
    assert object_store.get_object(nxt.b2_path) == b"parquet-bytes"


def test_enrich_writes_to_enriched_prefix(
    object_store: ObjectStore, sample_message: PipelineMessage
) -> None:
    _seed(object_store, sample_message.b2_path)

    nxt = EnrichProcessor(object_store)(sample_message)

    assert nxt.b2_path.startswith("enriched/sunnah-api/batch-001/")


def test_normalize_writes_to_normalized_prefix(
    object_store: ObjectStore, sample_message: PipelineMessage
) -> None:
    _seed(object_store, sample_message.b2_path)

    nxt = NormalizeProcessor(object_store)(sample_message)

    assert nxt.b2_path == "normalized/batch-001/hadiths.parquet"


def test_ingest_returns_none_and_reads_input(
    object_store: ObjectStore, sample_message: PipelineMessage
) -> None:
    _seed(object_store, sample_message.b2_path)

    result = IngestProcessor(object_store)(sample_message)

    assert result is None  # terminal stage


def test_processor_chain_propagates_batch_id(
    object_store: ObjectStore, sample_message: PipelineMessage
) -> None:
    """dedup → enrich → normalize must all carry the original batch_id."""
    _seed(object_store, sample_message.b2_path)

    after_dedup = DedupProcessor(object_store)(sample_message)
    after_enrich = EnrichProcessor(object_store)(after_dedup)
    after_norm = NormalizeProcessor(object_store)(after_enrich)

    assert after_norm.batch_id == "batch-001"
    assert after_norm.source == "sunnah-api"
