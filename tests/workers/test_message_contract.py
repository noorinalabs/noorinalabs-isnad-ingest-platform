"""Producer↔consumer contract regression net (#141, coupled with da#492 / da#503).

The authoritative message shape is THIS repo's ``PipelineMessage``
(``workers/lib/message.py``, ``extra="forbid"``). The data-acquisition
producer (da#503) is aligned to it and emits the wire form via its own
``serialize_message`` (``model_dump_json().encode`` → ISO-8601 timestamp,
integer ``record_count``).

This test deliberately does NOT ``import`` the da package (``src.messaging``):
a cross-repo import would couple the ip unit suite to another repo's checkout
and break CI in isolation. Instead it reconstructs the byte-exact wire message
the aligned producer emits (confirmed on-disk by Nikolaos, da#492) and asserts
the consumer's ``parse_message`` round-trips it cleanly. A future contract
drift on either side — a renamed, added, or removed field — becomes a red test
here rather than a silent prod crash in the dedup worker.
"""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from workers.lib.message import PipelineMessage, parse_message, serialize_message

# The exact JSON object the aligned da producer (da#503) writes to Kafka.
# raw-landed messages carry record_count=0 by design — the count is established
# downstream (Nikolaos, da#492).
PRODUCER_WIRE: dict[str, object] = {
    "batch_id": "9f8e7d6c-0000-4a1b-8c2d-1122334455aa",
    "source": "sunnah-api",
    "b2_path": "raw/sunnah-api/2026-07-26/hadiths.parquet",
    "timestamp": "2026-07-26T12:00:00Z",
    "record_count": 0,
}


def _producer_message() -> PipelineMessage:
    """The producer's message object — mirror of da ``src.messaging.PipelineMessage``."""
    return PipelineMessage.model_validate(PRODUCER_WIRE)


def test_producer_wire_bytes_roundtrip_through_consumer() -> None:
    """Producer serialize → consumer ``parse_message`` → full field parity."""
    wire_bytes = serialize_message(_producer_message())  # producer's serializer shape
    parsed = parse_message(wire_bytes)

    assert parsed.batch_id == PRODUCER_WIRE["batch_id"]
    assert parsed.source == PRODUCER_WIRE["source"]
    assert parsed.b2_path == PRODUCER_WIRE["b2_path"]
    assert parsed.record_count == PRODUCER_WIRE["record_count"]
    assert parsed.timestamp == _producer_message().timestamp


def test_consumer_parses_producer_wire_dict_directly() -> None:
    """The raw JSON object the producer emits parses without transformation."""
    parsed = parse_message(json.dumps(PRODUCER_WIRE).encode("utf-8"))
    assert parsed == _producer_message()


def test_roundtrip_is_identity() -> None:
    msg = _producer_message()
    assert parse_message(serialize_message(msg)) == msg


def test_contract_drift_extra_field_is_rejected() -> None:
    """A producer that adds an uncoordinated field must fail loudly (extra='forbid')."""
    drifted = {**PRODUCER_WIRE, "checksum": "deadbeef"}
    with pytest.raises(ValidationError):
        parse_message(json.dumps(drifted).encode("utf-8"))


def test_contract_drift_missing_required_field_is_rejected() -> None:
    """A producer that drops a required field must fail loudly."""
    drifted = {k: v for k, v in PRODUCER_WIRE.items() if k != "b2_path"}
    with pytest.raises(ValidationError):
        parse_message(json.dumps(drifted).encode("utf-8"))
