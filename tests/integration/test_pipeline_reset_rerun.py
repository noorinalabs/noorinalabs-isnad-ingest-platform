"""Admin stage-reset → rerun pipeline scenario (noorinalabs-main#136, #108).

#136 asks for pipeline worker integration scenarios. The chain itself and its
graph-load tail are ALREADY covered end-to-end:

  * ``tests/integration/test_kafka_worker_e2e.py`` (#55) drives
    enrich → normalize → ingest through REAL Kafka topics + MinIO + Neo4j
    testcontainers via the production ``WorkerRunner`` / ``build_runner``.
  * ``tests/integration/test_e2e_inprocess.py`` (#139) runs the whole chain
    in-process via ``scripts/e2e_pipeline_run.py``.

The one #136 acceptance bullet NEITHER of those covers is the admin
**reset → rerun** workflow (#108): "resets a given pipeline stage and
subsequent reruns succeed". ``tests/test_pipeline/test_reset.py`` unit-tests
the resetter internals (prefix deletion, Kafka offset reset, audit) but never
combines a reset with a subsequent worker-chain rerun. This module fills
exactly that gap — and nothing more, to avoid duplicating #55 / #139.

It is an in-process scenario (no Docker): the real ``DedupProcessor`` and the
real ``PipelineResetter`` operate over one shared in-memory object store, so
the reset's ``list_objects_v2`` / ``delete_objects`` surface and the worker's
``get`` / ``put`` / ``copy`` surface act on the same bucket — which is what
makes "reset then rerun against the same store" a faithful test.
"""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pytest

from src.pipeline.reset import PipelineResetter, ResetScope
from tests.workers.conftest import FakeS3Client, build_hadith_parquet
from workers.dedup.processor import DedupProcessor
from workers.lib.message import PipelineMessage
from workers.lib.object_store import ObjectStore

# A minimal well-formed batch. ML deps (dedup faiss) are absent in CI, so dedup
# pass-through-copies the hadith payload to its output prefix and writes an
# empty parallel_links side-table — enough to assert the stage produced output
# and that a rerun reproduces it after a reset.
_ROWS: list[dict[str, Any]] = [
    {
        "source_id": "sunnah:bukhari:1",
        "source_corpus": "sunnah",
        "collection_name": "bukhari",
        "book_number": 1,
        "chapter_number": 1,
        "hadith_number": 1,
        "matn_ar": "إنما الأعمال بالنيات",
        "matn_en": "Actions are judged by intentions.",
        "isnad_raw_ar": None,
        "isnad_raw_en": None,
        "full_text_ar": None,
        "full_text_en": None,
        "grade": "sahih",
        "chapter_name_ar": None,
        "chapter_name_en": "Revelation",
        "sect": "sunni",
    },
]


class _ResettableFakeS3(FakeS3Client):
    """``FakeS3Client`` (worker get/put/copy surface) plus the
    ``list_objects_v2`` / ``delete_objects`` surface ``PipelineResetter`` uses,
    so a SINGLE in-memory store backs both the worker chain and the admin
    reset. Mirrors the list/delete semantics of ``tests/test_pipeline/test_reset.py``.
    """

    def list_objects_v2(
        self, *, Bucket: str, Prefix: str, ContinuationToken: str | None = None
    ) -> dict[str, Any]:
        keys = sorted(k for (b, k) in self.objects if b == Bucket and k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}

    def delete_objects(self, *, Bucket: str, Delete: dict[str, Any]) -> dict[str, Any]:
        for obj in Delete["Objects"]:
            self.objects.pop((Bucket, obj["Key"]), None)
        return {"Deleted": Delete["Objects"]}


class _NoopKafka:
    def reset_consumer_offsets(self, topic: str, group_id: str) -> None: ...
    def delete_topic_data(self, topic: str) -> None: ...


class _NoopNeo4j:
    def truncate_hadith_graph(self) -> int:
        return 0


class _NoopPg:
    def truncate_hadith_metadata(self) -> int:
        return 0


def _keys_under(store: ObjectStore, prefix: str) -> list[str]:
    client: Any = store.client
    return [k for (b, k) in client.objects if b == store.bucket and k.startswith(prefix)]


class TestAdminStageResetThenRerun:
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
        """The #136 reset→rerun acceptance: a dedup-stage reset wipes the dedup
        prefix (leaving the raw input intact), and a subsequent rerun of the
        dedup worker repopulates it."""
        # --- run 1: dedup populates its output prefix ---
        store.put_object(raw_msg.b2_path, build_hadith_parquet(_ROWS))
        DedupProcessor(store)(raw_msg)
        assert _keys_under(store, "dedup/"), "dedup prefix should be populated after run 1"

        # --- admin resets the dedup stage ---
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
        # A dedup-stage reset must NOT touch the raw input — otherwise a rerun
        # would be impossible.
        assert _keys_under(store, "raw/"), "raw input must survive a dedup-stage reset"

        # --- subsequent rerun succeeds and repopulates the dedup prefix ---
        rerun_msg = DedupProcessor(store)(raw_msg)
        assert rerun_msg.b2_path == "dedup/sunnah-api/reset-batch-001/hadiths.parquet"
        assert _keys_under(store, "dedup/"), "rerun after reset must repopulate dedup prefix"
