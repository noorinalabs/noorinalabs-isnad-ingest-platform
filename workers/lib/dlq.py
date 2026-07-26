"""Dead-letter queue producer.

Workers route uncaught exceptions here rather than crashing the consumer
loop. Messages include the original pointer, worker identity, error
class, traceback, and a retry count so downstream tooling (see #108) can
replay or triage.

A message that cannot even be parsed into a :class:`PipelineMessage`
(malformed bytes, or a producer↔consumer contract drift under
``extra="forbid"``) has no structured ``original`` — it is quarantined
with ``original=None`` and its ``raw_payload`` captured verbatim so the
poison message can still be triaged/replayed (#141, BUG-03b).
"""

from __future__ import annotations

import json
import traceback
from datetime import UTC, datetime
from typing import Any

from workers.lib.message import PipelineMessage
from workers.lib.topics import PIPELINE_DLQ

__all__ = ["DLQ_TOPIC", "DLQRecord", "build_dlq_record"]

# Backwards-compatible alias — runner.py and tests still import DLQ_TOPIC.
# The canonical name is workers.lib.topics.PIPELINE_DLQ.
DLQ_TOPIC = PIPELINE_DLQ


def _coerce_raw_payload(raw: bytes | str | dict[str, Any] | None) -> str | None:
    """Render an unparseable raw message value as a triage-friendly string."""
    if raw is None:
        return None
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        return raw
    return json.dumps(raw, default=str)


class DLQRecord:
    """Tagged failure record. Serialised as JSON for ``pipeline.dlq``.

    Exactly one of ``original`` / ``raw_payload`` carries the failing
    message: ``original`` for a parsed :class:`PipelineMessage` (processing
    failure), ``raw_payload`` for a message that failed to parse.
    """

    def __init__(
        self,
        *,
        worker: str,
        original: PipelineMessage | None,
        error_class: str,
        error_message: str,
        error_traceback: str,
        retry_count: int = 0,
        raw_payload: str | None = None,
    ) -> None:
        self.worker = worker
        self.original = original
        self.error_class = error_class
        self.error_message = error_message
        self.error_traceback = error_traceback
        self.retry_count = retry_count
        self.raw_payload = raw_payload
        self.failed_at = datetime.now(UTC)

    def to_dict(self) -> dict[str, Any]:
        return {
            "worker": self.worker,
            "failed_at": self.failed_at.isoformat(),
            "retry_count": self.retry_count,
            "error_class": self.error_class,
            "error_message": self.error_message,
            "error_traceback": self.error_traceback,
            "original": (
                json.loads(self.original.model_dump_json()) if self.original is not None else None
            ),
            "raw_payload": self.raw_payload,
        }

    def to_bytes(self) -> bytes:
        return json.dumps(self.to_dict()).encode("utf-8")


def build_dlq_record(
    *,
    worker: str,
    original: PipelineMessage | None,
    exc: BaseException,
    retry_count: int = 0,
    raw_payload: bytes | str | dict[str, Any] | None = None,
) -> DLQRecord:
    """Build a :class:`DLQRecord` from an exception plus its failing message.

    ``original`` is the parsed pointer for a processing failure; for a parse
    failure pass ``original=None`` and the ``raw_payload`` that could not be
    parsed.
    """
    return DLQRecord(
        worker=worker,
        original=original,
        error_class=type(exc).__name__,
        error_message=str(exc),
        error_traceback="".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        retry_count=retry_count,
        raw_payload=_coerce_raw_payload(raw_payload),
    )
