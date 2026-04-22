"""Pin the public surface of ``workers.lib.topics``.

These asserts are deliberately blunt: every downstream consumer
(ingest, normalize, enrich, dedup, plus ``data-acquisition``'s
``kafka_producer``) depends on the exact wire-level string values
published here. A rename must be a conscious, cross-repo decision —
updating this test forces that conversation.
"""

from __future__ import annotations

from workers.lib import topics


def test_topic_names_are_stable_wire_constants() -> None:
    assert topics.PIPELINE_RAW_LANDED == "pipeline.raw.landed"
    assert topics.PIPELINE_DEDUP_DONE == "pipeline.dedup.done"
    assert topics.PIPELINE_ENRICH_DONE == "pipeline.enrich.done"
    assert topics.PIPELINE_NORMALIZE_DONE == "pipeline.normalize.done"
    assert topics.PIPELINE_DLQ == "pipeline.dlq"


def test_topic_names_follow_past_tense_pattern() -> None:
    """Per #192: ``pipeline.<stage>.<past-tense-event>`` for every stage topic.

    DLQ is excluded — it's a sink, not a stage event.
    """
    stage_topics = [
        topics.PIPELINE_RAW_LANDED,
        topics.PIPELINE_DEDUP_DONE,
        topics.PIPELINE_ENRICH_DONE,
        topics.PIPELINE_NORMALIZE_DONE,
    ]
    for t in stage_topics:
        parts = t.split(".")
        assert len(parts) == 3, f"{t!r} must have exactly 3 dot-separated parts"
        assert parts[0] == "pipeline", f"{t!r} must be namespaced under 'pipeline.'"


def test_allowed_node_labels_match_ingest_vocabulary() -> None:
    """The frozenset is the single source of truth for both normalize fan-out
    and ingest MERGE. Keep these names in sync with ``src/graph/load_nodes.py``.
    """
    assert topics.ALLOWED_NODE_LABELS == frozenset(
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


def test_dlq_topic_alias_still_exported_from_dlq_module() -> None:
    """Existing imports of ``workers.lib.dlq.DLQ_TOPIC`` must keep working."""
    from workers.lib.dlq import DLQ_TOPIC

    assert DLQ_TOPIC == topics.PIPELINE_DLQ
