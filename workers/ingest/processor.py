"""ingest-worker processor.

Terminal stage: loads the normalized Parquet into Neo4j and returns
``None`` so the runner does not publish a follow-on pointer.

TODO (follow-up issue): port ``src/graph/load_nodes.py`` (MERGE on ID
key, explicit SET per Phase 4 safety rule) and
``src/graph/load_edges.py`` (batch size 1000, edge types documented in
ontology). Current scaffold reads the batch for observability only and
leaves the actual Neo4j write as TODO — the driver connection and MERGE
statements are non-trivial and belong in a focused follow-up.
"""

from __future__ import annotations

from workers.lib.message import PipelineMessage
from workers.lib.object_store import ObjectStore

__all__ = ["IngestProcessor"]


class IngestProcessor:
    def __init__(self, store: ObjectStore, neo4j_driver: object | None = None) -> None:
        self.store = store
        self.neo4j_driver = neo4j_driver

    def __call__(self, msg: PipelineMessage) -> None:
        # Pull the object so failures to locate the batch surface before
        # we start a Neo4j transaction.
        _ = self.store.get_object(msg.b2_path)

        # TODO: open a Neo4j session via self.neo4j_driver, MERGE nodes
        # and CREATE edges in 1000-record batches, verify counts.
        return None
