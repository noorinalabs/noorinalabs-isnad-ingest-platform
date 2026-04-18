"""Test fakes for streaming workers — no Kafka, no S3, no Docker required."""

from __future__ import annotations

from typing import Any

import pytest

from workers.lib.message import PipelineMessage
from workers.lib.object_store import ObjectStore


class FakeProducer:
    """Collects ``(topic, value)`` tuples for assertion."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes]] = []

    def send(self, topic: str, value: bytes) -> None:
        self.sent.append((topic, value))


class FakeConsumer:
    """Iterable yielding pre-seeded records (each with a ``.value`` attr)."""

    class _Record:
        def __init__(self, value: Any) -> None:
            self.value = value

    def __init__(self, values: list[Any]) -> None:
        self._records = [self._Record(v) for v in values]

    def __iter__(self):
        return iter(self._records)


class FakeS3Client:
    """In-memory S3 client supporting the two methods ``ObjectStore`` uses."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        body = self.objects[(Bucket, Key)]

        class _Body:
            def __init__(self, data: bytes) -> None:
                self._data = data

            def read(self) -> bytes:
                return self._data

        return {"Body": _Body(body)}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str = "") -> None:
        self.objects[(Bucket, Key)] = Body


@pytest.fixture
def fake_s3() -> FakeS3Client:
    return FakeS3Client()


@pytest.fixture
def object_store(fake_s3: FakeS3Client) -> ObjectStore:
    return ObjectStore(bucket="test-bucket", client=fake_s3)


@pytest.fixture
def sample_message() -> PipelineMessage:
    return PipelineMessage(
        batch_id="batch-001",
        source="sunnah-api",
        b2_path="raw/sunnah-api/2026-04-13/hadiths.parquet",
        record_count=42,
    )
