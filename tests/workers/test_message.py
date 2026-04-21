"""PipelineMessage schema tests."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from workers.lib.message import PipelineMessage, parse_message, serialize_message


def test_roundtrip_json() -> None:
    original = PipelineMessage(
        batch_id="b1", source="src", b2_path="raw/x.parquet", record_count=10
    )
    blob = serialize_message(original)
    parsed = parse_message(blob)
    assert parsed == original


def test_parse_accepts_dict() -> None:
    msg = parse_message(
        {
            "batch_id": "b1",
            "source": "src",
            "b2_path": "raw/x.parquet",
            "timestamp": "2026-04-13T12:00:00+00:00",
            "record_count": 10,
        }
    )
    assert msg.record_count == 10


def test_record_count_must_be_non_negative() -> None:
    with pytest.raises(ValidationError):
        PipelineMessage(batch_id="b1", source="s", b2_path="p", record_count=-1)


def test_to_next_stage_preserves_batch_id_and_source() -> None:
    msg = PipelineMessage(batch_id="b1", source="src", b2_path="raw/x", record_count=5)
    nxt = msg.to_next_stage(b2_path="dedup/src/b1/x")
    assert nxt.batch_id == "b1"
    assert nxt.source == "src"
    assert nxt.b2_path == "dedup/src/b1/x"
    assert nxt.record_count == 5


def test_extra_fields_rejected() -> None:
    with pytest.raises(ValidationError):
        PipelineMessage(batch_id="b1", source="s", b2_path="p", record_count=1, unknown_field=True)
