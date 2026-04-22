"""normalize-worker processor — unify per-source schemas.

Goal
----
Produce a single unified hadith Parquet that the ingest worker can MERGE
into Neo4j without per-source branches. Each source parser in
``src/parse/*`` already emits rows that roughly follow
``HADITH_SCHEMA``, but:

* column order and nullability can drift between sources
* required non-null fields (``source_id``, ``source_corpus``,
  ``collection_name``, ``sect``) may occasionally be missing from a
  malformed batch
* extra columns produced by earlier stages (e.g. dedup side data) need
  to be stripped before graph load

This processor anchors on ``HADITH_SCHEMA`` from ``src/parse/schemas.py``
and performs:

1. Schema validation — every column required by the target schema must
   be present in the input.
2. Row filtering — rows missing any non-nullable target field are
   dropped and counted.
3. Column selection + cast — output is exactly ``HADITH_SCHEMA`` order /
   types, dropping any unexpected columns.

A rich cross-source mapping (e.g. flattening alternate isnad
representations) is deliberately out of scope — it will arrive in a
follow-up issue once ingest's Neo4j writes stabilise.
"""

from __future__ import annotations

import io

import pyarrow as pa
import pyarrow.parquet as pq

from src.parse.schemas import HADITH_SCHEMA
from workers.lib.log import get_logger
from workers.lib.message import PipelineMessage
from workers.lib.object_store import ObjectStore

__all__ = ["NormalizeProcessor"]

_logger = get_logger("workers.normalize")


def _required_fields(schema: pa.Schema) -> list[str]:
    return [f.name for f in schema if not f.nullable]


class NormalizeProcessor:
    """Callable processor: coerce a batch to ``HADITH_SCHEMA`` shape."""

    def __init__(self, store: ObjectStore, target_schema: pa.Schema = HADITH_SCHEMA) -> None:
        self.store = store
        self.target_schema = target_schema
        self._required = _required_fields(target_schema)

    def _normalize(self, payload: bytes) -> tuple[pa.Table, int]:
        """Return (normalized_table, dropped_row_count). Raises on unusable input."""
        table = pq.read_table(io.BytesIO(payload))

        missing = [f.name for f in self.target_schema if f.name not in table.column_names]
        if missing:
            raise ValueError(f"input parquet missing required columns: {missing}")

        projected = table.select([f.name for f in self.target_schema])

        # Drop rows with a null in any required-non-nullable field.
        dropped = 0
        if self._required:
            mask = pa.array([True] * projected.num_rows, type=pa.bool_())
            import pyarrow.compute as pc

            for name in self._required:
                not_null = pc.is_valid(projected.column(name))
                mask = pc.and_(mask, not_null)
            kept = projected.filter(mask)
            dropped = projected.num_rows - kept.num_rows
            projected = kept

        try:
            normalized = projected.cast(self.target_schema, safe=False)
        except pa.ArrowInvalid as exc:
            raise ValueError(f"cannot cast batch to HADITH_SCHEMA: {exc}") from exc

        return normalized, dropped

    def __call__(self, msg: PipelineMessage) -> PipelineMessage:
        payload = self.store.get_object(msg.b2_path)

        try:
            normalized, dropped = self._normalize(payload)
        except (pa.ArrowInvalid, ValueError) as exc:
            _logger.error("normalize_invalid_batch", batch_id=msg.batch_id, error=str(exc))
            raise

        out_key = f"normalized/{msg.batch_id}/hadiths.parquet"
        buf = io.BytesIO()
        pq.write_table(normalized, buf)
        self.store.put_object(out_key, buf.getvalue())

        _logger.info(
            "normalize_complete",
            batch_id=msg.batch_id,
            rows=normalized.num_rows,
            dropped=dropped,
        )
        return msg.to_next_stage(b2_path=out_key, record_count=normalized.num_rows)
