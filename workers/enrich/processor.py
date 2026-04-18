"""enrich-worker processor.

TODO (follow-up issue): port ``src/enrich/metrics.py``,
``src/enrich/topics.py``, ``src/enrich/historical.py``. Current scaffold
is a pass-through so the full pipeline can be exercised end-to-end
during integration testing.
"""

from __future__ import annotations

from workers.lib.message import PipelineMessage
from workers.lib.object_store import ObjectStore

__all__ = ["EnrichProcessor"]


class EnrichProcessor:
    def __init__(self, store: ObjectStore) -> None:
        self.store = store

    def __call__(self, msg: PipelineMessage) -> PipelineMessage:
        payload = self.store.get_object(msg.b2_path)

        # TODO: invoke graph metrics (PageRank / Louvain via GDS), topic
        # classifier (transformer 14-label), and historical event linker.
        enriched = payload

        out_key = f"enriched/{msg.source}/{msg.batch_id}/hadiths.parquet"
        self.store.put_object(out_key, enriched)

        return msg.to_next_stage(b2_path=out_key)
