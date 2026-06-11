"""In-process full-pipeline E2E — main#139 first light, no Docker required.

Companion to ``test_kafka_worker_e2e.py`` (which drives the same four stages
through a real Kafka broker + MinIO + Neo4j containers and is Docker-gated).
This module exercises the **whole chain** —
``acquire(sunnah) → raw → dedup → enrich → normalize → ingest → graph`` —
in one process via ``scripts/e2e_pipeline_run.py``, so it runs in the default
``pytest -m "not integration"`` lane with zero infra.

What is genuinely covered here that the per-stage unit tests are not
--------------------------------------------------------------------
* The **cross-stage topic topology** end to end: the pointer message
  traverses ``pipeline.raw.landed`` → ``.dedup.done`` → ``.enrich.done`` →
  ``.normalize.done`` and the terminal ingest stage MERGEs into the graph —
  with the production ``WorkerRunner`` driving each hop (DLQ routing,
  idempotency guard, metrics) and the production ``ObjectStore`` for every
  object hand-off.
* The **normalize → ingest contract**: normalize's D-ii fan-out (per-label
  Parquet + manifest-last) is consumed by ingest's manifest-gated MERGE, and
  the real ingest Cypher (node-first-then-edges, fail-loud on dangling edge
  endpoints per #22/#33) is executed against an in-process capture.
* **Zero DLQ** for a well-formed sunnah batch — a regression guard that the
  happy path stays clean.

This is NOT a substitute for the Docker E2E: the transport is an in-memory
broker and the Neo4j sink is an in-process emulator, so the real Kafka wire
and a live database are still proven only by ``test_kafka_worker_e2e.py``.
"""

from __future__ import annotations

from scripts.e2e_pipeline_run import (
    CapturingNeo4jDriver,
    build_sunnah_fixture,
    run_pipeline,
)


def test_full_chain_traverses_all_stages_no_dlq() -> None:
    driver = CapturingNeo4jDriver()
    summary = run_pipeline(driver)

    # Every stage produced exactly one downstream message; the terminal
    # stage is the only one with no produce topic.
    stages = {h["stage"]: h for h in summary["hops"]}
    assert set(stages) == {"dedup", "enrich", "normalize", "ingest"}
    for name in ("dedup", "enrich", "normalize"):
        assert stages[name]["messages_in"] == 1, name
        assert stages[name]["messages_out"] == 1, name
    assert stages["ingest"]["messages_in"] == 1
    assert stages["ingest"]["messages_out"] == 0  # terminal

    # No batch fell into the DLQ.
    assert summary["dlq_total"] == 0


def test_normalize_writes_manifest_last_with_all_labels() -> None:
    summary = run_pipeline(CapturingNeo4jDriver())
    manifest = summary["manifest"]
    assert manifest is not None, "normalize must write _MANIFEST.json"

    labels = {e.get("label") for e in manifest["parquets"] if e.get("label")}
    # The sunnah fixture exercises every fan-out node label.
    assert labels == {"Hadith", "Collection", "Chain", "Narrator", "Grading"}
    # Exactly one label-less (edges) entry.
    edge_entries = [e for e in manifest["parquets"] if "label" not in e]
    assert len(edge_entries) == 1


def test_graph_end_state_matches_fixture() -> None:
    rows = build_sunnah_fixture()
    summary = run_pipeline(CapturingNeo4jDriver())
    g = summary["graph"]

    # One Hadith node per input row; APPEARS_IN edge per row.
    #
    # NOTE: these are COUNT assertions only. This harness does NOT assert the
    # Hadith node *id* string, and deliberately so: with a realistic
    # corpus-prefixed fixture the streaming normalize worker currently emits a
    # DOUBLE-prefixed id (``hdt:sunnah:sunnah:bukhari:1:1``) — that is the
    # ig#63 bug, NOT the canonical contract. The canonical id is the batch
    # loader's single-prefix ``hdt:<source_id>`` (``hdt:sunnah:bukhari:1:1``),
    # confirmed against src/models/hadith.py + src/graph/load_nodes.py and the
    # live-loaded da#73 graph. Once ig#63 lands (normalize → single-prefix),
    # this test gains an explicit cross-path id-equality assertion
    # (batch == streaming). Until then it stays count-only so it neither
    # endorses the buggy id nor breaks on the in-flight fix.
    assert g["Hadith"] == len(rows)
    assert g["APPEARS_IN"] == len(rows)

    # Two distinct collections (bukhari, muslim) in the fixture.
    assert g["Collection"] == 2

    # The two rows carrying an English isnad produce narrators + chain edges
    # and gradings; the bare-matn row contributes only a Chain node.
    assert g["Chain"] == len(rows)
    assert g["Narrator"] >= 4
    assert g["TRANSMITTED_TO"] >= 2
    assert g["NARRATED"] == 2
    assert g["Grading"] == 2
    assert g["GRADED_BY"] == 2


def test_ingest_emitted_real_merge_cypher() -> None:
    """The terminal stage ran the production MERGE Cypher, not a stub."""
    driver = CapturingNeo4jDriver()
    run_pipeline(driver)
    joined = "\n".join(driver.cypher_log)
    # Node MERGE for Hadith + edge MERGE for APPEARS_IN must both appear.
    assert "MERGE (n:`Hadith` {id: row.id})" in joined
    assert "MERGE (s)-[r:`APPEARS_IN`]->(t)" in joined
    # Fail-loud edge guard: the edge Cypher returns a skipped count.
    assert "AS skipped" in joined
