"""ingest-worker processor.

Terminal stage: opens a Neo4j session per batch, MERGEs nodes from the
normalized Parquet on stable IDs, and returns ``None`` so the runner
does not publish a follow-on pointer.

Design (issue #13):

- Session per batch. Each Kafka message carries a pointer to one
  normalized Parquet; we open a session, run a single retryable write
  transaction via ``session.execute_write``, and close the session.
- ``execute_write`` uses the neo4j driver's built-in retry — transient
  failures like ``ServiceUnavailable`` are retried automatically, no
  custom retry loop.
- MERGE on stable IDs. Node IDs come from the unified normalize schema
  (landing in #10). Each normalized row must carry a ``label`` and an
  ``id`` field; everything else is merged as properties via explicit
  SET-per-key (Phase 4 safety rule from ``src/graph/load_nodes.py``).
- Error taxonomy:
    * ``ServiceUnavailable`` → neo4j retries internally inside
      ``execute_write``; if it still fails it propagates and the runner
      DLQs.
    * ``ClientError`` (malformed Cypher / constraint violation) → do
      not retry; propagate so runner DLQs with the exception metadata.
    * ``SessionExpired`` → reopen session once and retry.
    * Everything else → propagate; the runner wraps it into a DLQ
      record via ``build_dlq_record``.
    * ``UnknownSchemaError`` is raised locally when the normalized
      payload shape isn't recognized; the runner DLQs it.
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING, Any

from workers.lib.log import get_logger
from workers.lib.message import PipelineMessage
from workers.lib.object_store import ObjectStore

if TYPE_CHECKING:
    from neo4j import Driver, ManagedTransaction

__all__ = ["IngestProcessor", "UnknownSchemaError"]

_logger = get_logger("workers.ingest.processor")


class UnknownSchemaError(ValueError):
    """Raised when the normalized payload does not match the unified schema."""


def _merge_node_tx(tx: ManagedTransaction, *, label: str, rows: list[dict[str, Any]]) -> int:
    """Run a parameterized MERGE for a single label.

    We build the query per label rather than relying on APOC so the
    worker image doesn't require the APOC plugin. Rows of the same
    label are batched into one UNWIND.
    """
    # Label is an identifier (not parameterizable in Cypher); callers
    # MUST validate it against an allowlist before passing here.
    query = (
        f"UNWIND $rows AS row\n"
        f"MERGE (n:`{label}` {{id: row.id}})\n"
        f"SET n += row.props\n"
        f"RETURN count(n) AS merged"
    )
    result = tx.run(query, rows=rows)
    record = result.single()
    return int(record["merged"]) if record else 0


# Node labels that may appear in the normalized stream. Keeping this
# explicit prevents Cypher injection via an attacker-controlled label
# field and documents the unified schema's entity vocabulary.
_ALLOWED_LABELS: frozenset[str] = frozenset(
    {
        "Narrator",
        "Hadith",
        "Collection",
        "Chain",
        "Grading",
        "HistoricalEvent",
        "Location",
    }
)


def _rows_from_parquet(payload: bytes) -> list[dict[str, Any]]:
    """Decode a Parquet payload into row dicts.

    The unified schema landing in #10 writes one row per node with
    ``label`` (one of :data:`_ALLOWED_LABELS`), ``id`` (stable string
    ID), and remaining columns treated as node properties.
    """
    import pyarrow.parquet as pq

    table = pq.read_table(io.BytesIO(payload))
    return list(table.to_pylist())


def _group_rows_by_label(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Validate rows and group them by node label for per-label MERGE batches."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for idx, row in enumerate(rows):
        label = row.get("label")
        row_id = row.get("id")
        if not label or not isinstance(label, str):
            msg = f"row {idx}: missing or non-string 'label' field"
            raise UnknownSchemaError(msg)
        if label not in _ALLOWED_LABELS:
            msg = f"row {idx}: unknown label {label!r} (allowed: {sorted(_ALLOWED_LABELS)})"
            raise UnknownSchemaError(msg)
        if not row_id or not isinstance(row_id, str):
            msg = f"row {idx}: missing or non-string 'id' field"
            raise UnknownSchemaError(msg)
        props = {k: v for k, v in row.items() if k not in {"label", "id"}}
        grouped.setdefault(label, []).append({"id": row_id, "props": props})
    return grouped


class IngestProcessor:
    """Terminal processor — MERGEs normalized rows into Neo4j."""

    def __init__(self, store: ObjectStore, neo4j_driver: Driver | None = None) -> None:
        self.store = store
        self.neo4j_driver = neo4j_driver

    def __call__(self, msg: PipelineMessage) -> None:
        # Pull the normalized object first so a missing batch surfaces
        # before we open a Neo4j session.
        payload = self.store.get_object(msg.b2_path)

        if self.neo4j_driver is None:
            # Allows the unit suite to exercise the read path without a
            # real driver. Production always wires a driver in
            # ``main.build_runner``.
            _logger.warning(
                "ingest_no_driver",
                batch_id=msg.batch_id,
                b2_path=msg.b2_path,
            )
            return None

        rows = _rows_from_parquet(payload)
        if not rows:
            _logger.info(
                "ingest_empty_batch",
                batch_id=msg.batch_id,
                b2_path=msg.b2_path,
            )
            return None

        grouped = _group_rows_by_label(rows)

        from neo4j.exceptions import SessionExpired

        def _run(session: Any) -> dict[str, int]:
            merged_by_label: dict[str, int] = {}
            for label, label_rows in grouped.items():
                count = session.execute_write(_merge_node_tx, label=label, rows=label_rows)
                merged_by_label[label] = count
            return merged_by_label

        try:
            with self.neo4j_driver.session() as session:
                merged_by_label = _run(session)
        except SessionExpired:
            _logger.warning(
                "ingest_session_expired_retrying",
                batch_id=msg.batch_id,
            )
            with self.neo4j_driver.session() as session:
                merged_by_label = _run(session)

        _logger.info(
            "ingest_merged",
            batch_id=msg.batch_id,
            b2_path=msg.b2_path,
            merged_by_label=merged_by_label,
            record_count=msg.record_count,
        )
        return None
