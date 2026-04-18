"""dedup-worker processor.

Happy path:

1. Fetch input Parquet at ``msg.b2_path``
2. Compute dedup/parallel-link output (placeholder pass-through)
3. Write output to ``dedup/{source}/{batch_id}/hadiths.parquet``
4. Return next-stage pointer

TODO (follow-up issue): port the full logic from
``src/resolve/dedup.py`` — FAISS index build, variant-type tiering,
cross-sect detection. Current scaffold writes the input Parquet
unchanged so downstream stages have something to consume during
integration testing.
"""

from __future__ import annotations

from workers.lib.message import PipelineMessage
from workers.lib.object_store import ObjectStore

__all__ = ["DedupProcessor"]


class DedupProcessor:
    """Callable processor wired into :class:`workers.lib.runner.WorkerRunner`."""

    def __init__(self, store: ObjectStore) -> None:
        self.store = store

    def __call__(self, msg: PipelineMessage) -> PipelineMessage:
        payload = self.store.get_object(msg.b2_path)

        # TODO: invoke real dedup — load Parquet, build FAISS index,
        # classify variant pairs, emit PARALLEL_LINKS schema.
        deduped = payload

        out_key = f"dedup/{msg.source}/{msg.batch_id}/hadiths.parquet"
        self.store.put_object(out_key, deduped)

        return msg.to_next_stage(b2_path=out_key)
