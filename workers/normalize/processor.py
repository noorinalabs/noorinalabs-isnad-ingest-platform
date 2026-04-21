"""normalize-worker processor.

Goal: map every per-source schema onto a single unified schema so
ingest-worker can write Neo4j nodes/edges without source-specific
branches.

TODO (follow-up issue): define the unified schema (anchor on
``HADITH_SCHEMA`` + ``NARRATOR_MENTION_SCHEMA`` from
``src/resolve/schemas.py``) and implement per-source field mapping.
Current scaffold writes the input Parquet unchanged but to the
normalized prefix so pointers flow through.
"""

from __future__ import annotations

from workers.lib.message import PipelineMessage
from workers.lib.object_store import ObjectStore

__all__ = ["NormalizeProcessor"]


class NormalizeProcessor:
    def __init__(self, store: ObjectStore) -> None:
        self.store = store

    def __call__(self, msg: PipelineMessage) -> PipelineMessage:
        payload = self.store.get_object(msg.b2_path)

        # TODO: apply unified-schema mapping per source.
        normalized = payload

        out_key = f"normalized/{msg.batch_id}/hadiths.parquet"
        self.store.put_object(out_key, normalized)

        return msg.to_next_stage(b2_path=out_key)
