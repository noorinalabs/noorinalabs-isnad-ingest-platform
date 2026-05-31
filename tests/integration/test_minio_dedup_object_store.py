"""MinIO-backed object-store integration coverage for the dedup flow (#55 gap 2).

``workers/lib/object_store.py`` is the S3-compatible client every worker uses
to read batch Parquets and write stage outputs (B2 in prod, MinIO in local
dev). The unit suite exercises it only through ``FakeS3Client`` (in-memory).
This module drives the real ``ObjectStore`` (real ``boto3`` → a live MinIO
container) through the ``DedupProcessor`` so the S3 wire contract is actually
exercised:

* ``get_object`` spooling a real streaming body to a seekable file
  (`tempfile.SpooledTemporaryFile`) before handing it to pyarrow — the
  prod-faithful path the ``FakeS3Client`` fake was built to mimic;
* ``copy_object`` (server-side ``CopyObject``) for the dedup pass-through;
* ``put_object`` for the emitted ``parallel_links.parquet``.

ML deps (`sentence-transformers`, `faiss`) are NOT required: with them absent
the processor's documented graceful-degradation path still does the
pass-through copy and emits an empty links table, which is exactly the
object-store surface (`get` / `copy` / `put`) this gap is about. A second
test asserts the full read→copy→put round-trip independent of the processor.

Runtime requirement
-------------------
``@pytest.mark.integration`` — needs Docker (testcontainers spins up a MinIO
container). Excluded from the default ``pytest -m "not integration"`` run
(CI / ``make check``); runs via ``make test-integration`` where Docker is
available. ``importorskip`` on both ``testcontainers.minio`` and ``boto3``
keeps a Docker-less lane collecting cleanly.
"""

from __future__ import annotations

import io

import pyarrow.parquet as pq
import pytest

pytest.importorskip("testcontainers.minio")
pytest.importorskip("boto3")

import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from testcontainers.minio import MinioContainer  # noqa: E402

from src.parse.schemas import HADITH_SCHEMA  # noqa: E402
from src.resolve.schemas import PARALLEL_LINKS_SCHEMA  # noqa: E402
from tests.factories import build_hadith_table  # noqa: E402
from workers.dedup.processor import DedupProcessor  # noqa: E402
from workers.lib.message import PipelineMessage  # noqa: E402
from workers.lib.object_store import ObjectStore  # noqa: E402

pytestmark = pytest.mark.integration

TEST_BUCKET = "noorinalabs-pipeline"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def minio_container():
    """A MinIO container with deterministic credentials."""
    container = MinioContainer(access_key="minio", secret_key="minio12345")
    with container as minio:
        yield minio


@pytest.fixture
def minio_object_store(minio_container: MinioContainer) -> ObjectStore:
    """A real ``ObjectStore`` (boto3) pointed at the MinIO container.

    Creates the pipeline bucket first — ``ObjectStore`` assumes the bucket
    exists (it only does object-level ops). The boto3 client uses
    path-style addressing because MinIO does not serve virtual-host-style
    bucket subdomains.
    """
    cfg = minio_container.get_config()
    endpoint_url = f"http://{cfg['endpoint']}"

    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    client.create_bucket(Bucket=TEST_BUCKET)

    return ObjectStore(bucket=TEST_BUCKET, client=client)


def _seed_raw_hadiths(store: ObjectStore, key: str) -> None:
    """Write a small HADITH_SCHEMA batch to MinIO at ``key``."""
    table = build_hadith_table(
        [
            {"source_id": "h-1", "matn_en": "Actions are judged by intentions"},
            {"source_id": "h-2", "matn_en": "Whoever believes should say good or stay silent"},
        ]
    )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    store.put_object(key, buf.getvalue())


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_dedup_passes_through_and_emits_links_via_real_minio(
    minio_object_store: ObjectStore,
) -> None:
    """End-to-end dedup against real MinIO: get → copy → put all land.

    Exercises the object-store wire contract through the dedup processor.
    With ML deps absent the processor still copies the hadith payload to
    the dedup prefix and writes an empty ``parallel_links.parquet`` — the
    pass-through + emit surface that uses ``copy_object`` and
    ``put_object`` against the live container.
    """
    raw_key = "raw/sunnah/2026-04-21/hadiths.parquet"
    _seed_raw_hadiths(minio_object_store, raw_key)

    msg = PipelineMessage(
        batch_id="batch-minio-001",
        source="sunnah",
        b2_path=raw_key,
        record_count=2,
    )

    next_msg = DedupProcessor(minio_object_store)(msg)

    # The pass-through hadith payload landed under the dedup prefix and the
    # next-stage pointer addresses it.
    expected_hadiths = "dedup/sunnah/batch-minio-001/hadiths.parquet"
    expected_links = "dedup/sunnah/batch-minio-001/parallel_links.parquet"
    assert next_msg.b2_path == expected_hadiths

    # Read the copied hadiths back through the real store and confirm the
    # row count survived the server-side copy.
    with minio_object_store.get_object(expected_hadiths) as stream:
        copied = pq.read_table(stream)
    assert copied.num_rows == 2
    assert set(copied.column_names) == {f.name for f in HADITH_SCHEMA}

    # The links side-table exists and matches PARALLEL_LINKS_SCHEMA (empty
    # in the no-ML path, but a real, well-formed Parquet object).
    with minio_object_store.get_object(expected_links) as stream:
        links = pq.read_table(stream)
    assert {f.name for f in PARALLEL_LINKS_SCHEMA} <= set(links.column_names)


def test_object_store_round_trip_against_real_minio(
    minio_object_store: ObjectStore,
) -> None:
    """Direct put → get → copy → get round-trip on the real client.

    Isolates the ``object_store.py`` S3 surface from the processor so a
    regression in spool/copy semantics is attributable to the store, not
    the dedup logic. Covers the seekable-spool path (`get_object` returns a
    rewound `SpooledTemporaryFile`) and the server-side ``CopyObject``.
    """
    src_key = "raw/sunnah/2026-04-21/payload.parquet"
    dst_key = "dedup/sunnah/batch-minio-002/payload.parquet"
    _seed_raw_hadiths(minio_object_store, src_key)

    # get_object returns a seekable spool — pyarrow's footer-seek must work.
    with minio_object_store.get_object(src_key) as stream:
        assert stream.seekable()
        original = pq.read_table(stream)
    assert original.num_rows == 2

    # Server-side copy, then read the destination back.
    minio_object_store.copy_object(src_key, dst_key)
    with minio_object_store.get_object(dst_key) as stream:
        copied = pq.read_table(stream)
    assert copied.num_rows == original.num_rows

    # rename_object = copy + delete: the renamed key exists, the source is gone.
    renamed_key = "dedup/sunnah/batch-minio-002/payload.final.parquet"
    minio_object_store.rename_object(dst_key, renamed_key)
    with minio_object_store.get_object(renamed_key) as stream:
        assert pq.read_table(stream).num_rows == original.num_rows
    with pytest.raises(Exception):  # noqa: B017 — any S3 NoSuchKey-family error
        with minio_object_store.get_object(dst_key) as stream:
            stream.read()
