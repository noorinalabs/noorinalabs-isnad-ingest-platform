#!/usr/bin/env python3
"""End-to-end pipeline run — sunnah source → dedup → enrich → normalize → ingest.

main#139 first-light driver. Runs the **full** four-stage worker chain
in one process, proving the plumbing before scaling to all 8 sources and
before pointing the ingest stage at prod Neo4j.

What this exercises (production code, no shims in the data path)
---------------------------------------------------------------
* The four production processors — ``DedupProcessor``, ``EnrichProcessor``,
  ``NormalizeProcessor``, ``IngestProcessor`` — wrapped in the production
  ``WorkerRunner`` consumer loop (DLQ routing, idempotency guard, metrics).
* The production ``ObjectStore`` (``workers/lib/object_store.py``) over an
  in-memory S3 client, so the seekable-spool ``get_object`` path, server-side
  ``copy_object`` pass-through, and ``_write_atomic`` ``.part`` → rename all
  run exactly as against MinIO/B2.
* Real Kafka *pointer* messages (``PipelineMessage``) serialized to bytes and
  routed stage-to-stage through an in-memory topic broker that mimics the
  ``pipeline.raw.landed`` → ``.dedup.done`` → ``.enrich.done`` →
  ``.normalize.done`` topology. Topic names come from
  ``workers/lib/topics.py`` — never hardcoded here.
* The normalize D-ii fan-out (per-label Parquet + ``_MANIFEST.json`` written
  LAST) and the ingest manifest-gated MERGE emitting the real Cypher.

Two reasons this is a *driver*, not a replacement for the Docker E2E
-------------------------------------------------------------------
1. The transport here is an in-memory broker, not a real Kafka broker. The
   real-Kafka leg is covered by ``tests/integration/test_kafka_worker_e2e.py``
   (testcontainers, Docker-gated). This driver runs with **no Docker**.
2. The terminal Neo4j sink runs in one of two modes:
   * ``--neo4j-uri`` absent (default): an in-process ``CapturingNeo4jDriver``
     executes the real Cypher the ingest stage builds against a faithful
     in-memory graph that answers the validate queries (node/edge counts by
     label, property reads). This proves the MERGE statements are correct and
     that data "lands", without a container.
   * ``--neo4j-uri bolt://...`` present: the real ``neo4j`` driver is used and
     rows MERGE into a live database — use this the moment Docker or a remote
     Neo4j is reachable to complete the live-data leg.

dedup/enrich ML deps (sentence-transformers/faiss, transformers/torch) are
optional; absent, both stages degrade gracefully (pass-through + empty side
tables), so the *plumbing* — the thing main#139 asks to prove — is exercised
regardless. The harness reports which ML stages were live vs degraded.

Usage::

    uv run python scripts/e2e_pipeline_run.py            # in-process Neo4j capture
    uv run python scripts/e2e_pipeline_run.py --json      # machine-readable summary
    uv run python scripts/e2e_pipeline_run.py \
        --neo4j-uri bolt://localhost:7687 \
        --neo4j-user neo4j --neo4j-password changeme       # live Neo4j leg
"""

from __future__ import annotations

import argparse
import io
import json
import sys
import uuid
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from src.parse.schemas import HADITH_SCHEMA
from workers.dedup.processor import DedupProcessor
from workers.enrich.processor import EnrichProcessor
from workers.ingest.processor import IngestProcessor
from workers.lib.message import PipelineMessage, serialize_message
from workers.lib.object_store import ObjectStore
from workers.lib.runner import WorkerRunner, WorkerSettings
from workers.lib.topics import (
    PIPELINE_DEDUP_DONE,
    PIPELINE_DLQ,
    PIPELINE_ENRICH_DONE,
    PIPELINE_NORMALIZE_DONE,
    PIPELINE_RAW_LANDED,
)
from workers.normalize.processor import NormalizeProcessor

SOURCE = "sunnah"
BUCKET = "noorinalabs-pipeline"


# ---------------------------------------------------------------------------
# In-memory S3 (faithful to the methods ObjectStore calls — mirrors
# tests/workers/conftest.py FakeS3Client + FakeStreamingBody so the prod
# ObjectStore spool/copy/rename paths run unchanged).
# ---------------------------------------------------------------------------


class _StreamingBody(io.IOBase):
    """Non-seekable read-only body, faithful to botocore ``StreamingBody``."""

    def __init__(self, data: bytes) -> None:
        super().__init__()
        self._buf = io.BytesIO(data)

    def read(self, amt: int | None = None) -> bytes:
        return self._buf.read() if amt is None else self._buf.read(amt)

    def readable(self) -> bool:
        return True

    def close(self) -> None:
        self._buf.close()
        super().close()


class InMemoryS3:
    """In-memory S3 supporting exactly the surface ``ObjectStore`` uses."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        return {"Body": _StreamingBody(self.objects[(Bucket, Key)])}

    def put_object(  # noqa: N803
        self, *, Bucket: str, Key: str, Body: bytes, ContentType: str = ""
    ) -> None:
        self.objects[(Bucket, Key)] = Body

    def copy_object(  # noqa: N803
        self, *, Bucket: str, Key: str, CopySource: dict[str, str]
    ) -> dict[str, Any]:
        self.objects[(Bucket, Key)] = self.objects[(CopySource["Bucket"], CopySource["Key"])]
        return {}

    def delete_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        self.objects.pop((Bucket, Key), None)
        return {}

    def keys_under(self, prefix: str) -> list[str]:
        return sorted(k for (_b, k) in self.objects if k.startswith(prefix))


# ---------------------------------------------------------------------------
# In-memory Kafka broker + producer/consumer shims. The runner only calls
# ``producer.send(topic, value)`` and iterates the consumer; the shims keep
# the wire contract (bytes in, ``.value`` records out) identical to kafka-python.
# ---------------------------------------------------------------------------


class InMemoryBroker:
    def __init__(self) -> None:
        self.topics: dict[str, list[bytes]] = {}

    def append(self, topic: str, value: bytes) -> None:
        self.topics.setdefault(topic, []).append(value)

    def drain(self, topic: str) -> list[bytes]:
        msgs = self.topics.get(topic, [])
        self.topics[topic] = []
        return msgs


class BrokerProducer:
    def __init__(self, broker: InMemoryBroker) -> None:
        self._broker = broker

    def send(self, topic: str, value: bytes) -> None:
        self._broker.append(topic, value)


class _Record:
    def __init__(self, value: bytes) -> None:
        self.value = value


class ListConsumer:
    """Iterable over a fixed list of values — one bounded drain, like the
    test-only ``consumer_timeout_ms`` config in the Docker E2E."""

    def __init__(self, values: list[bytes]) -> None:
        self._records = [_Record(v) for v in values]

    def __iter__(self) -> Any:
        return iter(self._records)


# ---------------------------------------------------------------------------
# In-process Neo4j capture — runs the REAL Cypher the ingest stage builds.
# ---------------------------------------------------------------------------


class CapturingNeo4jDriver:
    """Executes ingest's real Cypher against an in-memory property graph.

    Supports exactly the Cypher shapes ``workers/ingest/processor.py`` emits
    (``UNWIND $rows ... MERGE (n:`Label` {id: row.id}) SET ... RETURN count``
    for nodes; the ``MATCH ... MATCH ... MERGE (s)-[r:`Label`]->(t)`` form for
    edges) plus the validate ``MATCH (n:Label) RETURN count(n)`` /
    ``MATCH ()-[r:Label]->() RETURN count(r)`` read queries. This is NOT a
    general Cypher engine — it is a faithful emulator of the ingest contract,
    so a correct MERGE statement lands a node/edge and a malformed one fails
    loudly. The cypher_log records every statement for evidence.
    """

    def __init__(self) -> None:
        self.nodes: dict[str, dict[str, Any]] = {}  # id -> {"labels": set, "props": {}}
        self.edges: dict[tuple[str, str, str], dict[str, Any]] = {}  # (label, src, dst) -> props
        self.cypher_log: list[str] = []
        self.closed = False

    # -- driver/session/tx surface the ingest processor drives --
    def session(self) -> _CapturingSession:
        return _CapturingSession(self)

    def verify_connectivity(self) -> None:  # parity with real driver entrypoint
        return None

    def close(self) -> None:
        self.closed = True

    # -- query execution --
    def _run(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        self.cypher_log.append(query)
        q = query.strip()
        rows = params.get("rows")
        if rows is not None and "MERGE (n:" in q:
            return self._merge_nodes(q, rows)
        if rows is not None and "MERGE (s)" in q:
            return self._merge_edges(q, rows)
        return self._read(q, params)

    @staticmethod
    def _label_in_backticks(q: str, after: str) -> str:
        # Extract the backticked label following a marker, e.g. ``MERGE (n:`Hadith` ``.
        idx = q.index(after)
        start = q.index("`", idx) + 1
        end = q.index("`", start)
        return q[start:end]

    def _merge_nodes(self, q: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        label = self._label_in_backticks(q, "MERGE (n:")
        for row in rows:
            node = self.nodes.setdefault(row["id"], {"labels": set(), "props": {}})
            node["labels"].add(label)
            # coalesce semantics: only overwrite when a non-null value is present
            for k, v in (row.get("props") or {}).items():
                if v is not None:
                    node["props"][k] = v
        return [{"merged": len(rows)}]

    def _merge_edges(self, q: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        # Faithful to the ingest edge Cypher (workers/ingest/processor.py
        # ``_build_edge_cypher``): OPTIONAL MATCH both endpoints, FOREACH-guard
        # the MERGE on both resolving, RETURN both ``merged`` and ``skipped``
        # counts. ``_merge_edges_tx`` raises EndpointMissingError on any
        # non-zero ``skipped`` (#22/#33 fail-loud), so the harness DLQs a batch
        # whose edges dangle — exactly as a real Neo4j would.
        label = self._label_in_backticks(q, "MERGE (s)-[r:")
        merged = 0
        skipped = 0
        for row in rows:
            src, dst = row["src_id"], row["dst_id"]
            if src not in self.nodes or dst not in self.nodes:
                skipped += 1
                continue
            edge = self.edges.setdefault((label, src, dst), {})
            for k, v in (row.get("props") or {}).items():
                if v is not None:
                    edge[k] = v
            merged += 1
        return [{"merged": merged, "skipped": skipped}]

    def _read(self, q: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        # MATCH (n:Label) RETURN count(n) AS cnt
        if "RETURN count(n)" in q and ":" in q:
            label = self._label_in_paren(q, "(n:")
            cnt = sum(1 for n in self.nodes.values() if label in n["labels"])
            return [{"cnt": cnt}]
        if "RETURN count(r)" in q:
            label = self._label_in_bracket(q)
            cnt = sum(1 for (lbl, _s, _d) in self.edges if lbl == label)
            return [{"cnt": cnt}]
        # MATCH (n:Hadith {id: $id}) RETURN n.matn_en AS matn_en
        if "{id: $id}" in q:
            node = self.nodes.get(params.get("id", ""))
            if node is None:
                return []
            return [{"matn_en": node["props"].get("matn_en")}]
        return []

    @staticmethod
    def _label_in_paren(q: str, after: str) -> str:
        idx = q.index(after) + len(after)
        rest = q[idx:]
        for stop in (")", " ", "{"):
            if stop in rest:
                rest = rest.split(stop, 1)[0]
        return rest.strip()

    @staticmethod
    def _label_in_bracket(q: str) -> str:
        # ...-[r:Label]->...
        idx = q.index("[r:") + len("[r:")
        return q[idx:].split("]", 1)[0].strip()


class _CapturingResult:
    def __init__(self, records: list[dict[str, Any]]) -> None:
        self._records = records

    def single(self) -> dict[str, Any] | None:
        return self._records[0] if self._records else None


class _CapturingTx:
    def __init__(self, driver: CapturingNeo4jDriver) -> None:
        self._driver = driver

    def run(self, query: str, **params: Any) -> _CapturingResult:
        return _CapturingResult(self._driver._run(query, params))


class _CapturingSession:
    def __init__(self, driver: CapturingNeo4jDriver) -> None:
        self._driver = driver

    def __enter__(self) -> _CapturingSession:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute_write(self, fn: Any, **kwargs: Any) -> Any:
        return fn(_CapturingTx(self._driver), **kwargs)

    def run(self, query: str, **params: Any) -> _CapturingResult:
        return _CapturingResult(self._driver._run(query, params))


# ---------------------------------------------------------------------------
# Fixture — a small but realistic sunnah batch (smallest source, first light).
# ---------------------------------------------------------------------------


def _hadith_row(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "source_id": "sunnah:bukhari:1:1",
        "source_corpus": SOURCE,
        "collection_name": "bukhari",
        "book_number": 1,
        "chapter_number": 1,
        "hadith_number": 1,
        "matn_ar": "إنما الأعمال بالنيات",
        "matn_en": (
            "Actions are judged by intentions, and every person will have what they intended."
        ),
        "isnad_raw_ar": None,
        "isnad_raw_en": None,
        "full_text_ar": None,
        "full_text_en": None,
        "grade": None,
        "chapter_name_ar": None,
        "chapter_name_en": None,
        "sect": "sunni",
    }
    base.update(overrides)
    return base


def build_sunnah_fixture() -> list[dict[str, Any]]:
    """A 3-row sunnah batch.

    Row 1: bare matn (sunnah_api shape — no isnad → Hadith+Collection+Chain only).
    Rows 2-3: carry ``isnad_raw_en`` (sunnah_scraped shape) so the
    Narrator / Chain / TRANSMITTED_TO / NARRATED + Grading/GRADED_BY fan-out
    is exercised end to end.
    """
    return [
        _hadith_row(
            source_id="sunnah:bukhari:1:1",
            matn_en=(
                "Actions are judged by intentions, and every person will have what they intended."
            ),
        ),
        _hadith_row(
            source_id="sunnah:bukhari:2:7",
            hadith_number=7,
            matn_en=(
                "None of you truly believes until he loves for his brother "
                "what he loves for himself."
            ),
            isnad_raw_en="Narrated by Umar ibn al-Khattab from Abu Hurairah from the Prophet",
            grade="sahih",
        ),
        _hadith_row(
            source_id="sunnah:muslim:3:12",
            collection_name="muslim",
            book_number=3,
            hadith_number=12,
            matn_en=(
                "Whoever believes in Allah and the Last Day should speak good or remain silent."
            ),
            isnad_raw_en="Narrated by Aisha from Abdullah ibn Umar from the Prophet",
            grade="sahih",
        ),
    ]


def hadith_rows_to_parquet(rows: list[dict[str, Any]]) -> bytes:
    by_col: dict[str, list[Any]] = {f.name: [] for f in HADITH_SCHEMA}
    for row in rows:
        for f in HADITH_SCHEMA:
            by_col[f.name].append(row.get(f.name))
    table = pa.table(by_col, schema=HADITH_SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Stage driver
# ---------------------------------------------------------------------------


@dataclass
class HopReport:
    stage: str
    consume_topic: str
    produce_topic: str | None
    messages_in: int
    messages_out: int
    dlq: int
    objects_written: list[str] = field(default_factory=list)
    note: str = ""


def _run_stage(
    *,
    stage: str,
    broker: InMemoryBroker,
    store: ObjectStore,
    s3: InMemoryS3,
    consume_topic: str,
    produce_topic: str | None,
    process: Any,
) -> HopReport:
    incoming = broker.drain(consume_topic)
    dlq_before = len(broker.topics.get(PIPELINE_DLQ, []))
    objs_before = set(s3.objects.keys())

    runner = WorkerRunner(
        settings=WorkerSettings(
            worker_name=f"{stage}-worker",
            consume_topic=consume_topic,
            produce_topic=produce_topic,
            consumer_group=f"{stage}-worker",
        ),
        consumer=ListConsumer(incoming),
        producer=BrokerProducer(broker),
        process=process,
    )
    runner.run_forever()

    out = len(broker.topics.get(produce_topic, [])) if produce_topic else 0
    dlq_after = len(broker.topics.get(PIPELINE_DLQ, []))
    new_objs = sorted(k for (_b, k) in (set(s3.objects.keys()) - objs_before))
    return HopReport(
        stage=stage,
        consume_topic=consume_topic,
        produce_topic=produce_topic,
        messages_in=len(incoming),
        messages_out=out,
        dlq=dlq_after - dlq_before,
        objects_written=new_objs,
    )


def run_pipeline(neo4j_driver: Any) -> dict[str, Any]:
    """Drive the four stages and return a structured evidence summary."""
    s3 = InMemoryS3()
    store = ObjectStore(bucket=BUCKET, client=s3)
    broker = InMemoryBroker()

    # --- Acquire/land: write the raw Parquet + seed pipeline.raw.landed ---
    rows = build_sunnah_fixture()
    batch_id = str(uuid.uuid4())
    raw_key = f"raw/{SOURCE}/2026-06-10/hadiths.parquet"
    store.put_object(raw_key, hadith_rows_to_parquet(rows))
    seed = PipelineMessage(
        batch_id=batch_id, source=SOURCE, b2_path=raw_key, record_count=len(rows)
    )
    broker.append(PIPELINE_RAW_LANDED, serialize_message(seed))

    hops: list[HopReport] = []
    hops.append(
        _run_stage(
            stage="dedup",
            broker=broker,
            store=store,
            s3=s3,
            consume_topic=PIPELINE_RAW_LANDED,
            produce_topic=PIPELINE_DEDUP_DONE,
            process=DedupProcessor(store),
        )
    )
    hops.append(
        _run_stage(
            stage="enrich",
            broker=broker,
            store=store,
            s3=s3,
            consume_topic=PIPELINE_DEDUP_DONE,
            produce_topic=PIPELINE_ENRICH_DONE,
            process=EnrichProcessor(store),
        )
    )
    hops.append(
        _run_stage(
            stage="normalize",
            broker=broker,
            store=store,
            s3=s3,
            consume_topic=PIPELINE_ENRICH_DONE,
            produce_topic=PIPELINE_NORMALIZE_DONE,
            process=NormalizeProcessor(store),
        )
    )
    hops.append(
        _run_stage(
            stage="ingest",
            broker=broker,
            store=store,
            s3=s3,
            consume_topic=PIPELINE_NORMALIZE_DONE,
            produce_topic=None,
            process=IngestProcessor(store, neo4j_driver=neo4j_driver),
        )
    )

    # --- Validate the graph end state via the same read queries the API uses ---
    graph_counts = _validate_graph(neo4j_driver)

    total_dlq = sum(h.dlq for h in hops)
    return {
        "batch_id": batch_id,
        "source": SOURCE,
        "rows_in": len(rows),
        "hops": [h.__dict__ for h in hops],
        "dlq_total": total_dlq,
        "graph": graph_counts,
        "manifest": _read_manifest(s3, batch_id),
    }


def _validate_graph(driver: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    with driver.session() as session:
        for label in ("Hadith", "Collection", "Narrator", "Chain", "Grading"):
            rec = session.run(f"MATCH (n:{label}) RETURN count(n) AS cnt").single()
            counts[label] = int(rec["cnt"]) if rec else 0
        for edge in ("APPEARS_IN", "TRANSMITTED_TO", "NARRATED", "GRADED_BY"):
            rec = session.run(f"MATCH ()-[r:{edge}]->() RETURN count(r) AS cnt").single()
            counts[edge] = int(rec["cnt"]) if rec else 0
    return counts


def _read_manifest(s3: InMemoryS3, batch_id: str) -> dict[str, Any] | None:
    key = f"normalized/{batch_id}/_MANIFEST.json"
    raw = s3.objects.get((BUCKET, key))
    return json.loads(raw) if raw else None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_neo4j_driver(args: argparse.Namespace) -> Any:
    if not args.neo4j_uri:
        return CapturingNeo4jDriver()
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password))
    driver.verify_connectivity()
    return driver


def _print_human(summary: dict[str, Any], *, live_neo4j: bool) -> None:
    print("=" * 70)
    print(f"main#139 E2E pipeline run — source={summary['source']} batch={summary['batch_id']}")
    print("=" * 70)
    print(f"Neo4j sink: {'LIVE' if live_neo4j else 'in-process capture (CapturingNeo4jDriver)'}")
    print(f"Rows acquired → raw/: {summary['rows_in']}")
    print()
    print("Per-hop traversal:")
    for h in summary["hops"]:
        arrow = h["produce_topic"] or "(terminal → Neo4j)"
        print(f"  {h['stage']:<10} {h['consume_topic']:<24} → {arrow}")
        print(
            f"             msgs in={h['messages_in']} out={h['messages_out']} "
            f"dlq={h['dlq']} objects+={len(h['objects_written'])}"
        )
        for obj in h["objects_written"]:
            print(f"               · {obj}")
    print()
    print(f"DLQ entries (must be 0): {summary['dlq_total']}")
    print()
    print("Graph end state (Neo4j):")
    for k, v in summary["graph"].items():
        print(f"  {k:<16} {v}")
    print("=" * 70)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--neo4j-uri", default=None, help="bolt:// URI for a live Neo4j sink")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default="changeme")
    parser.add_argument("--json", action="store_true", help="emit a machine-readable summary")
    args = parser.parse_args(argv)

    if args.json:
        # Worker structlog defaults to a stdout PrintLogger; redirect it to
        # stderr so stdout carries ONLY the JSON summary (pipe-parseable).
        import structlog

        structlog.configure(logger_factory=structlog.PrintLoggerFactory(file=sys.stderr))

    driver = _build_neo4j_driver(args)
    live = args.neo4j_uri is not None
    try:
        summary = run_pipeline(driver)
    finally:
        driver.close()

    if args.json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        _print_human(summary, live_neo4j=live)

    # Non-zero exit if the pipeline failed to land the expected end state,
    # so the script doubles as a CI/smoke check.
    g = summary["graph"]
    ok = (
        summary["dlq_total"] == 0
        and g["Hadith"] == summary["rows_in"]
        and g["Collection"] >= 1
        and g["APPEARS_IN"] == summary["rows_in"]
    )
    if not ok:
        print("E2E FAILED: end-state assertions not met", file=sys.stderr)
        return 1
    # To stderr so ``--json`` stdout stays pure JSON.
    print(
        "E2E OK: full chain traversed; expected nodes/edges landed in Neo4j.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
