"""Dead-letter queue producer.

Workers route uncaught exceptions here rather than crashing the consumer
loop. Messages include the original pointer, worker identity, error
class, traceback, and a retry count so downstream tooling (see #108) can
replay or triage.
"""

from __future__ import annotations

import json
import traceback
from datetime import UTC, datetime
from typing import Any

from workers.lib.message import PipelineMessage

__all__ = ["DLQRecord", "build_dlq_record"]

DLQ_TOPIC = "pipeline.dlq"


class DLQRecord:
    """Tagged failure record. Serialised as JSON for ``pipeline.dlq``."""

    def __init__(
        self,
        *,
        worker: str,
        original: PipelineMessage,
        error_class: str,
        error_message: str,
        error_traceback: str,
        retry_count: int = 0,
    ) -> None:
        self.worker = worker
        self.original = original
        self.error_class = error_class
        self.error_message = error_message
        self.error_traceback = error_traceback
        self.retry_count = retry_count
        self.failed_at = datetime.now(UTC)

    def to_dict(self) -> dict[str, Any]:
        return {
            "worker": self.worker,
            "failed_at": self.failed_at.isoformat(),
            "retry_count": self.retry_count,
            "error_class": self.error_class,
            "error_message": self.error_message,
            "error_traceback": self.error_traceback,
            "original": json.loads(self.original.model_dump_json()),
        }

    def to_bytes(self) -> bytes:
        return json.dumps(self.to_dict()).encode("utf-8")


def build_dlq_record(
    *, worker: str, original: PipelineMessage, exc: BaseException, retry_count: int = 0
) -> DLQRecord:
    """Build a :class:`DLQRecord` from an exception plus its original pointer."""
    return DLQRecord(
        worker=worker,
        original=original,
        error_class=type(exc).__name__,
        error_message=str(exc),
        error_traceback="".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        retry_count=retry_count,
    )
