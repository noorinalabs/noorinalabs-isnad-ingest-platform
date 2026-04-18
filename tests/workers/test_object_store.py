"""ObjectStore wrapper tests against the in-memory fake S3 client."""

from __future__ import annotations

from tests.workers.conftest import FakeS3Client
from workers.lib.object_store import ObjectStore


def test_put_then_get_roundtrip() -> None:
    fake = FakeS3Client()
    store = ObjectStore(bucket="b", client=fake)

    store.put_object("raw/x.parquet", b"hello")
    assert store.get_object("raw/x.parquet") == b"hello"


def test_bucket_is_passed_through() -> None:
    fake = FakeS3Client()
    store = ObjectStore(bucket="my-bucket", client=fake)
    store.put_object("k", b"v")
    assert ("my-bucket", "k") in fake.objects
