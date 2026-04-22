"""Pipeline pointer-message schema.

Kafka messages are lightweight pointers, not data payloads. The actual
records live in S3-compatible storage (B2 in prod, MinIO for local dev)
and the message carries a ``b2_path`` that identifies the object.

Schema matches the contract defined in issue #106:

.. code-block:: json

    {
      "batch_id": "uuid",
      "source": "sunnah-api",
      "b2_path": "raw/sunnah-api/2026-04-13/hadiths.parquet",
      "timestamp": "2026-04-13T12:00:00+00:00",
      "record_count": 1234
    }
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

__all__ = ["PipelineMessage", "parse_message", "serialize_message"]


class PipelineMessage(BaseModel):
    """Pointer message flowing between pipeline stages."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    batch_id: str = Field(..., description="UUID identifying this processing batch")
    source: str = Field(..., description="Data source identifier, e.g. 'sunnah-api'")
    b2_path: str = Field(
        ...,
        description=(
            "S3 path. Two forms (both valid — the consumer stage dictates which): "
            "(1) a single object key, e.g. 'raw/sunnah-api/2026-04-13/x.parquet'; "
            "(2) a folder prefix with a trailing slash, e.g. 'normalized/<batch_id>/', "
            "used by the normalize→ingest hand-off (#192 D-ii) where ingest reads "
            "'_MANIFEST.json' under the prefix to discover the per-label Parquets."
        ),
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="When this message was produced (ISO-8601 UTC)",
    )
    record_count: int = Field(..., ge=0, description="Number of records in the referenced object")

    def to_next_stage(self, *, b2_path: str, record_count: int | None = None) -> PipelineMessage:
        """Derive the next-stage message, preserving ``batch_id`` and ``source``."""
        return PipelineMessage(
            batch_id=self.batch_id,
            source=self.source,
            b2_path=b2_path,
            timestamp=datetime.now(UTC),
            record_count=record_count if record_count is not None else self.record_count,
        )


def parse_message(raw: bytes | str | dict[str, Any]) -> PipelineMessage:
    """Parse a Kafka message value into a :class:`PipelineMessage`."""
    if isinstance(raw, (bytes, str)):
        payload = json.loads(raw)
    else:
        payload = raw
    return PipelineMessage.model_validate(payload)


def serialize_message(msg: PipelineMessage) -> bytes:
    """Serialize a :class:`PipelineMessage` to bytes for Kafka."""
    return msg.model_dump_json().encode("utf-8")
