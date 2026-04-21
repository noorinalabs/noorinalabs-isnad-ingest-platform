"""DLQ record structure tests."""

from __future__ import annotations

import json

from workers.lib.dlq import build_dlq_record
from workers.lib.message import PipelineMessage


def test_dlq_record_includes_traceback_and_original() -> None:
    original = PipelineMessage(batch_id="b1", source="s", b2_path="raw/x", record_count=1)
    try:
        raise ValueError("bad data")
    except ValueError as exc:
        record = build_dlq_record(worker="test-worker", original=original, exc=exc)

    payload = json.loads(record.to_bytes())
    assert payload["worker"] == "test-worker"
    assert payload["error_class"] == "ValueError"
    assert payload["error_message"] == "bad data"
    assert "Traceback" in payload["error_traceback"]
    assert payload["original"]["batch_id"] == "b1"
    assert payload["retry_count"] == 0
