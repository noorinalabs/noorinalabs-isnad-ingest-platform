"""Test fakes for streaming workers — no Kafka, no S3, no Docker required."""

from __future__ import annotations

import io
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.parse.schemas import HADITH_SCHEMA
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


def _hadith_row(
    *,
    source_id: str,
    source_corpus: str = "sunnah",
    collection_name: str = "bukhari",
    sect: str = "sunni",
    matn_en: str | None = None,
    matn_ar: str | None = None,
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "source_corpus": source_corpus,
        "collection_name": collection_name,
        "book_number": 1,
        "chapter_number": 1,
        "hadith_number": 1,
        "matn_ar": matn_ar,
        "matn_en": matn_en,
        "isnad_raw_ar": None,
        "isnad_raw_en": None,
        "full_text_ar": None,
        "full_text_en": None,
        "grade": None,
        "chapter_name_ar": None,
        "chapter_name_en": None,
        "sect": sect,
    }


def build_hadith_parquet(rows: list[dict[str, Any]]) -> bytes:
    """Serialize a list of HADITH_SCHEMA rows to Parquet bytes."""
    if not rows:
        table = HADITH_SCHEMA.empty_table()
    else:
        by_col: dict[str, list[Any]] = {f.name: [] for f in HADITH_SCHEMA}
        for row in rows:
            for f in HADITH_SCHEMA:
                by_col[f.name].append(row.get(f.name))
        table = pa.table(by_col, schema=HADITH_SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


@pytest.fixture
def hadith_row() -> Any:
    """Row factory — yields the helper so tests can build custom rows."""
    return _hadith_row


@pytest.fixture
def sample_hadith_parquet() -> bytes:
    """Two well-formed rows using HADITH_SCHEMA — enough for most processor tests."""
    return build_hadith_parquet(
        [
            _hadith_row(
                source_id="sunnah:001",
                matn_en=(
                    "Actions are judged by intentions and every person will have "
                    "only what they intended."
                ),
            ),
            _hadith_row(
                source_id="sunnah:002",
                matn_en=(
                    "Whoever believes in Allah and the Last Day should say what "
                    "is good or remain silent."
                ),
            ),
        ]
    )
