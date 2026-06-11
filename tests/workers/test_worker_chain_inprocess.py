"""CI-durable in-process worker-chain scenarios (main#136).

The companion ``tests/integration/test_worker_chain_e2e.py`` drives the same
four stages through real Kafka + MinIO + Neo4j containers, but is Docker-gated
(``@pytest.mark.integration``) so CI never runs it. This module exercises the
**worker-chain contract** —
``dedup → enrich → normalize → ingest`` — entirely in-process on the faithful
fake stack from ``tests/workers/conftest.py`` (``FakeS3Client`` →
``ObjectStore`` + parquet builders), so the chain is a regression guard that
runs in the default ``pytest -m "not integration"`` lane with zero infra.

The chain contract reused here (per the existing in-process unit tests)::

    after_dedup     = DedupProcessor(store)(msg)
    after_enrich    = EnrichProcessor(store)(after_dedup)
    after_normalize = NormalizeProcessor(store)(after_enrich)
    IngestProcessor(store, neo4j_driver=<fake>)(after_normalize)   # terminal

What this adds over the pre-existing ``test_processor_chain_propagates_batch_id``
-------------------------------------------------------------------------------
That test stops at normalize and asserts only batch-id propagation. This
module carries the chain through the **ingest graph-load tail** and asserts the
actual graph end-state, using a *faithful* in-process Neo4j fake (``FakeNeo4j``)
that — unlike a Cypher-logging stub — actually tracks node ids so the ingest
edge MERGE's ``OPTIONAL MATCH`` endpoint-resolution semantics are reproduced.
That fidelity is what lets the in-process lane guard two contracts the unit
suite otherwise leaves to the Docker E2E:

* **Hadith-id parity (#63)** — the corpus-prefixed fixture
  (``source_id="sunnah:bukhari:1:1"``) must MERGE as ``hdt:sunnah:bukhari:1:1``
  (the batch loader's ``hdt:{source_id}``), never the doubled
  ``hdt:sunnah:sunnah:bukhari:1:1``.
* **Fail-loud edge guard (#22/#33)** — because ``FakeNeo4j`` resolves edge
  endpoints against the nodes actually MERGEd in the same batch, a happy-path
  chain produces zero skipped edges and the terminal stage does not raise.

ML deps are NOT required: dedup and enrich degrade gracefully (the chain
forces ``_ml_unavailable`` to keep CI deterministic without
``sentence-transformers`` / ``transformers``), so the hadith payload reaches
normalize and ingest on every hop.
"""

from __future__ import annotations

import io
import json
import re
from typing import Any

import pyarrow.parquet as pq
import pytest

from workers.dedup.processor import DedupProcessor
from workers.enrich.processor import EnrichProcessor
from workers.ingest.processor import IngestProcessor
from workers.lib.message import PipelineMessage
from workers.lib.object_store import ObjectStore
from workers.normalize.processor import NormalizeProcessor

# A corpus-prefixed source_id, exactly as ``generate_source_id`` emits it.
# This is what makes the #63 doubling observable end to end — a bare "h-1"
# cannot.
SOURCE = "sunnah"
SOURCE_ID = "sunnah:bukhari:1:1"
EXPECTED_HADITH_ID = f"hdt:{SOURCE_ID}"

# Discriminates a node MERGE from an edge MERGE by the Cypher shape ingest
# emits (``_build_node_cypher``: ``MERGE (n:`Label` {id: row.id})``).
_NODE_MERGE_RE = re.compile(r"MERGE \(n:`\w+` \{id: row\.id\}\)")


# ---------------------------------------------------------------------------
# Faithful in-process Neo4j fake
# ---------------------------------------------------------------------------


class _FakeResult:
    def __init__(self, record: dict[str, int] | None) -> None:
        self._record = record

    def single(self) -> dict[str, int] | None:
        return self._record


class _FakeTx:
    """Reproduces the slice of the Neo4j tx surface ingest uses: ``run`` of a
    node-MERGE or edge-MERGE Cypher, returning a single record with the same
    ``merged`` / ``skipped`` counts a real database would.

    Node ids are tracked in a shared set so the edge query's OPTIONAL MATCH
    endpoint resolution is faithful: an edge whose ``src_id``/``dst_id`` was
    never MERGEd counts as ``skipped`` — the exact condition the production
    ``_merge_edges_tx`` fails loud on (#22/#33).
    """

    def __init__(self, node_ids: set[str]) -> None:
        self._node_ids = node_ids

    def run(self, query: str, rows: list[dict[str, Any]] | None = None, **_: Any) -> _FakeResult:
        rows = rows or []
        if _NODE_MERGE_RE.search(query):
            for row in rows:
                self._node_ids.add(row["id"])
            return _FakeResult({"merged": len(rows)})
        # Edge MERGE: resolve both endpoints against MERGEd node ids.
        merged = sum(
            1 for r in rows if r["src_id"] in self._node_ids and r["dst_id"] in self._node_ids
        )
        skipped = len(rows) - merged
        return _FakeResult({"merged": merged, "skipped": skipped})


class _FakeSession:
    def __init__(self, node_ids: set[str]) -> None:
        self._node_ids = node_ids

    def __enter__(self) -> _FakeSession:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute_write(self, fn: Any) -> Any:
        return fn(_FakeTx(self._node_ids))


class FakeNeo4j:
    """In-process stand-in for a ``neo4j.Driver`` with a queryable graph.

    Tracks every MERGEd node id so tests can assert the exact graph end-state
    (e.g. the canonical Hadith id) without a live database.
    """

    def __init__(self) -> None:
        self.node_ids: set[str] = set()

    def session(self) -> _FakeSession:
        return _FakeSession(self.node_ids)

    def close(self) -> None:  # parity with the real driver surface
        return None

    @property
    def hadith_ids(self) -> list[str]:
        return sorted(nid for nid in self.node_ids if nid.startswith("hdt:"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_raw_batch(store: ObjectStore, key: str, rows: list[dict[str, Any]]) -> None:
    """Write a HADITH_SCHEMA Parquet batch to the fake store at ``key``."""
    from tests.factories import build_hadith_table

    table = build_hadith_table(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    store.put_object(key, buf.getvalue())


def _run_chain(
    store: ObjectStore, msg: PipelineMessage, driver: FakeNeo4j
) -> dict[str, PipelineMessage | None]:
    """Drive dedup → enrich → normalize → ingest in-process on the fake stack.

    ML is forced unavailable so the chain is deterministic in CI without the
    optional embedding/classification deps — the documented graceful-degradation
    path still does the pass-through copy + manifest fan-out the chain needs.
    """
    dedup = DedupProcessor(store)
    dedup._ml_unavailable = True  # type: ignore[attr-defined]
    enrich = EnrichProcessor(store)
    enrich._ml_unavailable = True  # type: ignore[attr-defined]
    normalize = NormalizeProcessor(store)
    ingest = IngestProcessor(store, neo4j_driver=driver)

    after_dedup = dedup(msg)
    after_enrich = enrich(after_dedup)
    after_normalize = normalize(after_enrich)
    after_ingest = ingest(after_normalize)
    return {
        "dedup": after_dedup,
        "enrich": after_enrich,
        "normalize": after_normalize,
        "ingest": after_ingest,
    }


def _read_manifest(store: ObjectStore, prefix: str) -> dict[str, Any]:
    body = store.get_object(f"{prefix}_MANIFEST.json").read()
    result: dict[str, Any] = json.loads(body.decode("utf-8"))
    return result


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_chain_traverses_all_four_stages_into_graph(
    object_store: ObjectStore,
    sample_message: PipelineMessage,
) -> None:
    """A raw batch flows dedup → enrich → normalize → ingest in-process and the
    Hadith node lands in the (fake) graph; the terminal stage returns None.
    """
    _seed_raw_batch(
        object_store,
        sample_message.b2_path,
        [{"source_id": SOURCE_ID, "source_corpus": SOURCE, "matn_en": "Actions are by intentions"}],
    )
    driver = FakeNeo4j()

    hops = _run_chain(object_store, sample_message, driver)

    # batch_id / source survive every hop (the pre-existing propagation guard).
    assert hops["normalize"].batch_id == sample_message.batch_id  # type: ignore[union-attr]
    assert hops["normalize"].source == sample_message.source  # type: ignore[union-attr]
    # Terminal ingest stage returns None.
    assert hops["ingest"] is None
    # The Hadith node reached the graph.
    assert EXPECTED_HADITH_ID in driver.node_ids


def test_chain_merges_canonical_hadith_id_no_doubled_corpus(
    object_store: ObjectStore,
    sample_message: PipelineMessage,
) -> None:
    """End-to-end #63 guard on the CI-durable lane.

    The corpus-prefixed fixture must MERGE as ``hdt:sunnah:bukhari:1:1`` — the
    batch loader's ``hdt:{source_id}`` — with no doubled corpus segment.
    """
    _seed_raw_batch(
        object_store,
        sample_message.b2_path,
        [{"source_id": SOURCE_ID, "source_corpus": SOURCE, "matn_en": "text"}],
    )
    driver = FakeNeo4j()

    _run_chain(object_store, sample_message, driver)

    assert driver.hadith_ids == [EXPECTED_HADITH_ID], (
        f"graph Hadith ids {driver.hadith_ids!r} != [{EXPECTED_HADITH_ID!r}] — "
        "streaming and batch must MERGE the same node (#63)"
    )
    assert all("sunnah:sunnah" not in nid for nid in driver.node_ids), "corpus doubled (#63)"


def test_chain_normalize_emits_full_manifest_consumed_by_ingest(
    object_store: ObjectStore,
    sample_message: PipelineMessage,
) -> None:
    """The normalize → ingest object contract holds across the in-process chain.

    A row carrying an English isnad + a grade exercises every fan-out label, and
    ingest must consume the manifest-gated per-label Parquets without raising the
    fail-loud edge guard (every edge endpoint MERGEd in the same batch).
    """
    rows = [
        {
            "source_id": SOURCE_ID,
            "source_corpus": SOURCE,
            "collection_name": "bukhari",
            "matn_en": "text",
            "grade": "sahih",
            "isnad_raw_en": "Narrated Abu Hurayrah from Malik on the authority of Nafi",
        }
    ]
    _seed_raw_batch(object_store, sample_message.b2_path, rows)
    driver = FakeNeo4j()

    hops = _run_chain(object_store, sample_message, driver)

    # normalize fanned out into every node label.
    manifest = _read_manifest(object_store, hops["normalize"].b2_path)  # type: ignore[union-attr]
    labels = {e.get("label") for e in manifest["parquets"] if e.get("label")}
    assert labels == {"Hadith", "Collection", "Chain", "Narrator", "Grading"}

    # ingest MERGEd the Hadith, its Collection, and at least two narrators —
    # and did NOT raise, proving every edge endpoint resolved (#22/#33).
    assert EXPECTED_HADITH_ID in driver.node_ids
    assert any(nid.startswith("col:") for nid in driver.node_ids)
    assert sum(1 for nid in driver.node_ids if nid.startswith("nar:")) >= 2
    assert hops["ingest"] is None


def test_chain_is_idempotent_on_replay(
    object_store: ObjectStore,
    sample_message: PipelineMessage,
) -> None:
    """Re-running the whole chain on the same batch yields the same graph ids.

    Stage outputs use deterministic ids (``hdt:{source_id}``, hashed narrator/
    chain ids), so a replay MERGEs onto the same nodes — the idempotency the
    pipeline relies on for at-least-once Kafka delivery.
    """
    _seed_raw_batch(
        object_store,
        sample_message.b2_path,
        [{"source_id": SOURCE_ID, "source_corpus": SOURCE, "matn_en": "text", "grade": "sahih"}],
    )

    driver1 = FakeNeo4j()
    _run_chain(object_store, sample_message, driver1)
    first = set(driver1.node_ids)

    driver2 = FakeNeo4j()
    _run_chain(object_store, sample_message, driver2)
    second = set(driver2.node_ids)

    assert first == second, "replaying the chain must produce identical node ids"
    assert EXPECTED_HADITH_ID in first


def test_fake_neo4j_edge_guard_matches_production_skip_semantics() -> None:
    """Guard the fake's own fidelity: an edge to an un-MERGEd endpoint is skipped.

    This pins the property that makes the chain tests meaningful — ``FakeNeo4j``
    reproduces the OPTIONAL MATCH endpoint resolution, so the production
    ``_merge_edges_tx`` fail-loud path (#22/#33) is actually reachable in-process
    rather than rubber-stamped. ``IngestProcessor`` raises ``EndpointMissingError``
    when an edge references a node absent from the graph.
    """
    from workers.ingest.processor import EndpointMissingError, _merge_edges_tx

    driver = FakeNeo4j()
    with driver.session() as session:
        # MERGE one node, then an edge whose dst is absent → must count skipped,
        # which _merge_edges_tx turns into EndpointMissingError.
        def _attempt(tx: Any) -> Any:
            tx.run("MERGE (n:`Hadith` {id: row.id})", rows=[{"id": "hdt:present"}])
            return _merge_edges_tx(
                tx,
                label="APPEARS_IN",
                rows=[
                    {"src_id": "hdt:present", "dst_id": "col:absent", "props": {}},
                ],
            )

        with pytest.raises(EndpointMissingError, match="absent from the graph"):
            session.execute_write(_attempt)
