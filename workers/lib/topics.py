"""Pipeline Kafka topic constants and node-label vocabulary.

Single source of truth for topic names across every worker and every
upstream producer (e.g. ``noorinalabs-data-acquisition``'s
``kafka_producer.py``). All pipeline constants live here so a rename
requires exactly one edit.

Naming convention (locked in #192):

    pipeline.<stage>.<past-tense-event>

``<past-tense-event>`` describes the state of the batch after the stage
finishes (``.landed``, ``.done``). ``.dlq`` is the terminal failure
sink, one topic shared by all workers.

The node-label vocabulary (``ALLOWED_NODE_LABELS``) lives here rather
than in ``workers/ingest/processor.py`` so the normalize stage can
validate its fan-out output against the same set ingest will MERGE with,
without creating a normalize→ingest import cycle.
"""

from __future__ import annotations

__all__ = [
    "ALLOWED_NODE_LABELS",
    "PIPELINE_DEDUP_DONE",
    "PIPELINE_DLQ",
    "PIPELINE_ENRICH_DONE",
    "PIPELINE_NORMALIZE_DONE",
    "PIPELINE_RAW_LANDED",
]

PIPELINE_RAW_LANDED: str = "pipeline.raw.landed"
PIPELINE_DEDUP_DONE: str = "pipeline.dedup.done"
PIPELINE_ENRICH_DONE: str = "pipeline.enrich.done"
PIPELINE_NORMALIZE_DONE: str = "pipeline.normalize.done"
PIPELINE_DLQ: str = "pipeline.dlq"

# Node labels the normalize stage may emit and the ingest stage may MERGE.
# Kept explicit to prevent Cypher-label injection and to document the
# unified entity vocabulary shared by the pipeline.
ALLOWED_NODE_LABELS: frozenset[str] = frozenset(
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
