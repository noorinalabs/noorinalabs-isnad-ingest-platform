"""Focused real-MinIO regression guard for ``reset._delete_prefix`` (#69).

``src/pipeline/reset._delete_prefix`` wipes a stage's objects during an admin
reset. It used the bulk ``delete_objects`` (S3 ``DeleteObjects``) op, which
boto3 (botocore>=1.36) no longer auto-signs with a ``Content-MD5``/checksum
header — so real S3-compatible stores (MinIO, Backblaze B2) reject it with
``MissingContentMD5``. AWS S3 is lenient, and the unit suite's ``FakeS3Client``
is in-memory, so the defect only shows against a *real* store.

The full ``dedup→…→ingest`` chain E2E
(``test_worker_chain_e2e.test_admin_stage_reset_wipes_only_normalized_prefix_on_real_minio``)
also guards this, but stands up THREE containers (Kafka, MinIO, Neo4j). This
module isolates the object-store delete path against a SINGLE MinIO container so
the regression is caught fast, without the chain or the Kafka/Neo4j stack — a
test that would have failed (``MissingContentMD5``) before the per-object
``delete_object`` fix and passes after.

Runtime requirement
-------------------
``@pytest.mark.integration`` — needs Docker (testcontainers spins up a MinIO
container). Excluded from the default ``pytest -m "not integration"`` run;
runs via ``make test-integration``. ``importorskip`` on
``testcontainers.minio`` / ``boto3`` keeps a Docker-less lane collecting cleanly.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

pytest.importorskip("testcontainers.minio")
pytest.importorskip("boto3")

import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from testcontainers.minio import MinioContainer  # noqa: E402

from src.pipeline.reset import _delete_prefix  # noqa: E402
from workers.lib.object_store import ObjectStore  # noqa: E402

pytestmark = pytest.mark.integration

TEST_BUCKET = "noorinalabs-pipeline"


@pytest.fixture(scope="module")
def minio_container() -> Iterator[MinioContainer]:
    with MinioContainer(access_key="minio", secret_key="minio12345") as minio:
        yield minio


@pytest.fixture
def minio_store(minio_container: MinioContainer) -> ObjectStore:
    """Real ``ObjectStore`` (boto3) on the MinIO container, bucket pre-created.

    Path-style addressing because MinIO does not serve virtual-host bucket
    subdomains.
    """
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


def _keys_under(store: ObjectStore, prefix: str) -> list[str]:
    resp = store.client.list_objects_v2(Bucket=store.bucket, Prefix=prefix)
    return sorted(obj["Key"] for obj in resp.get("Contents", []) or [])


def test_delete_prefix_against_real_minio_no_missing_content_md5(
    minio_store: ObjectStore,
) -> None:
    """``_delete_prefix`` wipes only its prefix on a real store, no MD5 error.

    Before the fix this raised ``botocore ... MissingContentMD5`` on the bulk
    ``delete_objects`` call; the per-object ``delete_object`` path carries no
    such requirement, so the prefix is emptied and the sibling prefix survives.
    """
    for key in ("normalized/sunnah/b1/f.parquet", "normalized/sunnah/b2/f.parquet"):
        minio_store.put_object(key, b"x")
    # A sibling prefix that must NOT be touched by the scoped delete.
    minio_store.put_object("raw/sunnah/b1/f.parquet", b"x")

    assert len(_keys_under(minio_store, "normalized/")) == 2

    deleted = _delete_prefix(minio_store, "normalized/")

    assert deleted == 2
    assert _keys_under(minio_store, "normalized/") == []
    assert _keys_under(minio_store, "raw/") == ["raw/sunnah/b1/f.parquet"]


def test_delete_prefix_paginates_past_single_response_on_real_minio(
    minio_store: ObjectStore,
) -> None:
    """Deletes every key when the listing spans multiple list_objects_v2 pages.

    S3/MinIO cap ``list_objects_v2`` at 1000 keys per response; seed >1000 so
    the resetter's continuation-token loop is exercised against the real store
    and every object is removed (not just the first page).
    """
    page = "paged/"
    seeded = [f"{page}{i:05d}.parquet" for i in range(1001)]
    for key in seeded:
        minio_store.put_object(key, b"x")

    deleted = _delete_prefix(minio_store, page)

    assert deleted == len(seeded)
    assert _keys_under(minio_store, page) == []
