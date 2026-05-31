"""ingest-worker processor — manifest-gated MERGE into Neo4j.

Terminal stage. Consumes a pointer to a folder prefix written by the
normalize worker (see ``workers/normalize/processor.py`` and #192
Option D-ii) and MERGEs the per-label Parquets into Neo4j in a single
retryable transaction: all node labels first, edges last.

Design (issue #18, #192 D-ii)
-----------------------------
* ``msg.b2_path`` is a folder prefix (e.g. ``normalized/<batch_id>/``).
  Ingest reads ``_MANIFEST.json`` from that prefix as the ready signal;
  normalize writes the manifest LAST so its presence implies every
  per-label Parquet already landed.
* Manifest entries are iterated in the order normalize emitted them
  (node labels first, edges last). Ingest re-sorts defensively so an
  out-of-order manifest can't cause MATCH-before-MERGE on edges.
* Single Neo4j session per batch. All MERGEs run inside one
  ``session.execute_write`` closure so the driver's built-in retry
  covers the whole batch atomically.
* Per-field enumerated SET. Each label has an allow-list in
  ``workers.ingest.schema.NODE_PROPERTY_MAP``; the generated Cypher
  SETs exactly those fields as ``n.field = row.props.field``. Closes
  Farhan's #192 Phase-4 flag: ``SET n += row.props`` let an
  attacker-controlled property overwrite the node, and also wiped
  scholar-curated fields not present in the current batch.
* Error taxonomy:
    * ``ManifestMissingError`` — subclass of ``UnknownSchemaError``;
      runner DLQs it.
    * ``UnknownSchemaError`` — manifest references an unknown label or
      an unknown edge type; runner DLQs.
    * Transient driver errors (``ServiceUnavailable``, ``TransientError``,
      ``SessionExpired``) — the neo4j driver retries them internally
      inside ``session.execute_write``, bounded by the driver-config
      ``max_transaction_retry_time`` (default 30s). Once that wall-clock
      bound is exhausted the last error propagates and the runner DLQs
      it. Per-message Neo4j retry time is therefore capped at ~30s.
    * ``ClientError`` and everything else — propagate for DLQ.

Issue #20 removed a redundant outer ``SessionExpired`` reopen that
compounded with the driver-internal retry; ``SessionExpired.is_retryable()``
returns True (verified in ``neo4j.exceptions`` and the ``_run_transaction``
retry loop in ``neo4j._sync.work.session``), so the driver already
retries the same call internally — re-running the batch on a fresh
session doubled the worst-case retry budget without adding fault
tolerance.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from workers.ingest.schema import (
    ALLOWED_EDGE_LABELS,
    EDGE_PROPERTY_MAP,
    NODE_PROPERTY_MAP,
)
from workers.lib.log import get_logger
from workers.lib.message import PipelineMessage
from workers.lib.object_store import ObjectStore
from workers.lib.topics import ALLOWED_NODE_LABELS

if TYPE_CHECKING:
    from neo4j import Driver, ManagedTransaction

__all__ = [
    "EndpointMissingError",
    "IngestProcessor",
    "ManifestMissingError",
    "UnknownSchemaError",
]

_logger = get_logger("workers.ingest.processor")

_MANIFEST_FILENAME = "_MANIFEST.json"


class UnknownSchemaError(ValueError):
    """Raised when the manifest / payload does not match the D-ii schema."""


class ManifestMissingError(UnknownSchemaError):
    """Raised when ``_MANIFEST.json`` is absent or cannot be parsed.

    Subclass of ``UnknownSchemaError`` so the runner's existing DLQ
    routing for schema errors catches this without a new branch.
    """


class EndpointMissingError(UnknownSchemaError):
    """Raised when an edge MERGE references an endpoint absent from the graph.

    #22 acceptance requires that a missing endpoint (``src_id`` or
    ``dst_id`` that does not resolve to an existing node at MERGE time)
    fail loud rather than silently skip. The old ``MATCH … MATCH … MERGE``
    idiom dropped such rows with no error, no telemetry, no DLQ — an
    undetectable data-loss class for the never-landing-endpoint case (see
    #33). The edge Cypher now ``OPTIONAL MATCH``es both endpoints and
    counts the rows where either side is null; a non-zero count raises
    this error.

    Subclass of ``UnknownSchemaError`` so the runner's existing DLQ
    routing for schema errors catches this without a new branch.
    """


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


def _normalize_prefix(b2_path: str) -> str:
    """Normalize ``msg.b2_path`` to a folder prefix with a trailing ``/``.

    The D-ii message contract uses a folder prefix (e.g.
    ``normalized/<batch_id>/``) but a caller that forgets the trailing
    slash shouldn't silently stringify-concat against the filename.
    """
    return b2_path if b2_path.endswith("/") else f"{b2_path}/"


def _read_manifest(store: ObjectStore, prefix: str) -> dict[str, Any]:
    """Fetch and parse ``_MANIFEST.json`` from ``prefix``.

    The manifest is small (a few KB at most), so we materialize it via
    ``.read()`` on the streaming body rather than passing the stream to
    a JSON parser — ``json.loads`` accepts only bytes/str.
    """
    key = f"{prefix}{_MANIFEST_FILENAME}"
    try:
        with store.get_object(key) as stream:
            raw = stream.read()
    except Exception as exc:  # noqa: BLE001 — surface any read failure as missing manifest
        msg = f"manifest not readable at {key!r}: {exc}"
        raise ManifestMissingError(msg) from exc
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        msg = f"manifest at {key!r} is not valid JSON: {exc}"
        raise ManifestMissingError(msg) from exc
    if not isinstance(parsed, dict):
        msg = f"manifest at {key!r} is not a JSON object"
        raise ManifestMissingError(msg)
    entries = parsed.get("parquets")
    if not isinstance(entries, list):
        msg = f"manifest at {key!r} missing 'parquets' list"
        raise ManifestMissingError(msg)
    return parsed


def _split_manifest_entries(
    entries: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Separate node entries from the edges entry and validate labels.

    Normalize writes node entries first and the edges file last; we
    still re-sort defensively so an out-of-order manifest can't cause
    edge MERGE to run before its node endpoints exist.
    """
    node_entries: list[dict[str, Any]] = []
    edges_entry: dict[str, Any] | None = None
    for idx, entry in enumerate(entries):
        if not isinstance(entry, dict) or "path" not in entry:
            msg = f"manifest entry {idx} missing 'path'"
            raise UnknownSchemaError(msg)
        label = entry.get("label")
        if label is None:
            # No label → this is the edges file. Manifest can only
            # carry one edges file per batch.
            if edges_entry is not None:
                msg = "manifest carries multiple label-less (edges) entries"
                raise UnknownSchemaError(msg)
            edges_entry = entry
            continue
        if not isinstance(label, str) or label not in ALLOWED_NODE_LABELS:
            msg = (
                f"manifest entry {idx} has unknown node label {label!r} "
                f"(allowed: {sorted(ALLOWED_NODE_LABELS)})"
            )
            raise UnknownSchemaError(msg)
        node_entries.append(entry)
    return node_entries, edges_entry


# ---------------------------------------------------------------------------
# Parquet row decoding
# ---------------------------------------------------------------------------


def _rows_from_parquet(stream: Any) -> list[dict[str, Any]]:
    """Decode a streaming Parquet body into row dicts.

    ``stream`` is the file-like body returned by ``ObjectStore.get_object``
    (botocore ``StreamingBody`` in prod). ``pq.read_table`` accepts any
    ``.read``-bearing object, so we avoid buffering the per-label Parquet
    in worker memory before parsing.
    """
    import pyarrow.parquet as pq

    table = pq.read_table(stream)
    return list(table.to_pylist())


def _decode_props(raw: Any) -> dict[str, Any]:
    """Decode the ``props`` column cell into a dict.

    Normalize encodes props as a JSON string to sidestep an exhaustive
    per-label Arrow schema. If a future writer produces dict cells
    directly we accept that too.
    """
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, str)):
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError as exc:
            msg = f"props cell is not valid JSON: {exc}"
            raise UnknownSchemaError(msg) from exc
        if not isinstance(decoded, dict):
            msg = f"props cell decoded to {type(decoded).__name__}, expected object"
            raise UnknownSchemaError(msg)
        return decoded
    msg = f"props cell has unexpected type {type(raw).__name__}"
    raise UnknownSchemaError(msg)


def _node_rows_for_merge(label: str, raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Validate node rows and shape them for the MERGE UNWIND.

    The Cypher builder reads ``row.id`` and ``row.props.<field>`` for
    each allow-listed field; we pre-strip anything outside the
    allow-list here so the wire payload is as small as the query allows.
    """
    allowed = NODE_PROPERTY_MAP.get(label, [])
    shaped: list[dict[str, Any]] = []
    for idx, raw in enumerate(raw_rows):
        row_id = raw.get("id")
        if not row_id or not isinstance(row_id, str):
            msg = f"{label} row {idx}: missing or non-string 'id'"
            raise UnknownSchemaError(msg)
        props = _decode_props(raw.get("props"))
        filtered = {k: props[k] for k in allowed if k in props}
        dropped = [k for k in props if k not in allowed]
        if dropped:
            _logger.warning(
                "ingest_dropped_unknown_props",
                label=label,
                row_id=row_id,
                dropped=dropped,
            )
        shaped.append({"id": row_id, "props": filtered})
    return shaped


def _edge_rows_for_merge(raw_rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Group edge rows by relationship label and shape them for UNWIND."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for idx, raw in enumerate(raw_rows):
        label = raw.get("label")
        if not label or not isinstance(label, str):
            msg = f"edge row {idx}: missing or non-string 'label'"
            raise UnknownSchemaError(msg)
        if label not in ALLOWED_EDGE_LABELS:
            msg = (
                f"edge row {idx}: unknown label {label!r} (allowed: {sorted(ALLOWED_EDGE_LABELS)})"
            )
            raise UnknownSchemaError(msg)
        src_id = raw.get("src_id")
        dst_id = raw.get("dst_id")
        if not isinstance(src_id, str) or not src_id:
            msg = f"edge row {idx}: missing or non-string 'src_id'"
            raise UnknownSchemaError(msg)
        if not isinstance(dst_id, str) or not dst_id:
            msg = f"edge row {idx}: missing or non-string 'dst_id'"
            raise UnknownSchemaError(msg)
        props = _decode_props(raw.get("props"))
        allowed = EDGE_PROPERTY_MAP.get(label, [])
        filtered = {k: props[k] for k in allowed if k in props}
        grouped.setdefault(label, []).append(
            {"src_id": src_id, "dst_id": dst_id, "props": filtered}
        )
    return grouped


# ---------------------------------------------------------------------------
# Cypher builders
# ---------------------------------------------------------------------------


def _build_node_cypher(label: str) -> str:
    """Per-label MERGE Cypher with enumerated, null-safe SET stanzas.

    The label is an identifier (Cypher cannot parameterize it) but we
    only reach here with labels validated against
    ``ALLOWED_NODE_LABELS``, so backtick-injection of the label is safe.
    Field names are also drawn from the server-side allow-list, not the
    incoming payload.

    Each SET uses ``coalesce(row.props.<f>, n.<f>)`` so a row that omits
    a field does NOT wipe the existing node property — the scholar-
    curated ``text_ar`` on a hadith survives an ingest batch that only
    carries ``text_en``. This is the core of Farhan's Phase-4 fix
    (#192 comment thread).

    Coalesce-clear contract (per-field, applies to every SET line)
    --------------------------------------------------------------
    - field omitted from ``row.props``  → preserve existing value
    - field present with explicit null  → preserve existing value (silent no-op)
    - field present with a new value    → overwrite

    The explicit-null branch is asymmetric on purpose: ingest batches
    cannot clear node properties. A correction that needs to null a
    field must reach Neo4j through a separate path (issue #23 / future
    corrections topic). The same contract applies to
    ``_build_edge_cypher``.
    """
    fields = NODE_PROPERTY_MAP.get(label, [])
    set_lines = ",\n    ".join(f"n.{f} = coalesce(row.props.{f}, n.{f})" for f in fields)
    set_clause = f"SET {set_lines}\n" if fields else ""
    return (
        "UNWIND $rows AS row\n"
        f"MERGE (n:`{label}` {{id: row.id}})\n"
        f"{set_clause}"
        "RETURN count(n) AS merged"
    )


def _build_edge_cypher(label: str) -> str:
    """Per-edge-label MERGE Cypher with fail-loud missing-endpoint detection.

    #22 acceptance: *"endpoint missing (both src_id and dst_id must exist
    — MERGE on node first or fail loud)"*. The original idiom

        MATCH (s {id: row.src_id})
        MATCH (t {id: row.dst_id})
        MERGE (s)-[r:`label`]->(t)

    silently dropped a row whose endpoint did not yet exist — the MATCH
    returned 0 rows and the relationship MERGE never fired. Within a
    single batch this is safe (node MERGE runs first in the same tx), but
    a cross-batch edge whose endpoint was supposed to land earlier and
    never did was lost with no error, no DLQ, no metric (#33).

    This builder uses ``OPTIONAL MATCH`` on both endpoints and a
    ``FOREACH`` guard that only MERGEs the relationship when both sides
    resolved. It returns two counts — the number of relationships merged
    and the number of rows whose endpoint was missing. The caller
    (``_merge_edges_tx``) raises ``EndpointMissingError`` when the missing
    count is non-zero, so the silent-skip becomes a loud DLQ.

    Coalesce-clear contract (per SET line) — same as ``_build_node_cypher``:

    - field omitted from ``row.props``  → preserve existing value
    - field present with explicit null  → preserve existing value (silent no-op)
    - field present with a new value    → overwrite

    Edge properties cannot be cleared via an ingest batch (see #23).
    """
    fields = EDGE_PROPERTY_MAP.get(label, [])
    # SET stanzas run inside the FOREACH guard, so they reference the
    # relationship bound there (``r``) and the per-row ``row`` captured by
    # the surrounding UNWIND.
    set_lines = ",\n        ".join(f"r.{f} = coalesce(row.props.{f}, r.{f})" for f in fields)
    set_clause = f"\n        SET {set_lines}" if fields else ""
    return (
        "UNWIND $rows AS row\n"
        "OPTIONAL MATCH (s {id: row.src_id})\n"
        "OPTIONAL MATCH (t {id: row.dst_id})\n"
        # FOREACH over a 1- or 0-element list: the relationship MERGE runs
        # only when BOTH endpoints resolved. A missing endpoint leaves the
        # list empty so nothing is created — and we count it below.
        "FOREACH (_ IN CASE WHEN s IS NOT NULL AND t IS NOT NULL THEN [1] ELSE [] END |\n"
        f"    MERGE (s)-[r:`{label}`]->(t){set_clause}\n"
        ")\n"
        "RETURN\n"
        "  count(CASE WHEN s IS NOT NULL AND t IS NOT NULL THEN 1 END) AS merged,\n"
        "  count(CASE WHEN s IS NULL OR t IS NULL THEN 1 END) AS skipped"
    )


def _merge_nodes_tx(tx: ManagedTransaction, *, label: str, rows: list[dict[str, Any]]) -> int:
    query = _build_node_cypher(label)
    result = tx.run(query, rows=rows)
    record = result.single()
    return int(record["merged"]) if record else 0


def _merge_edges_tx(tx: ManagedTransaction, *, label: str, rows: list[dict[str, Any]]) -> int:
    """MERGE one edge label's rows; fail loud on any missing endpoint.

    The edge Cypher (see ``_build_edge_cypher``) returns both the merged
    count and a ``skipped`` count of rows whose ``src_id``/``dst_id`` did
    not resolve to a node in the graph. Per #22 acceptance a missing
    endpoint must fail loud, so any non-zero ``skipped`` raises
    ``EndpointMissingError`` — the runner then DLQs the whole batch
    rather than letting the edge data vanish silently (#33).
    """
    query = _build_edge_cypher(label)
    result = tx.run(query, rows=rows)
    record = result.single()
    if record is None:
        return 0
    skipped = int(record["skipped"]) if record["skipped"] is not None else 0
    if skipped:
        missing = _missing_endpoint_ids(rows)
        _logger.error(
            "ingest_edge_endpoint_missing",
            edge_label=label,
            skipped=skipped,
            total=len(rows),
            missing_endpoints=missing,
        )
        msg = (
            f"{label}: {skipped} of {len(rows)} edge row(s) reference an "
            f"endpoint absent from the graph at MERGE time "
            f"(missing node ids: {missing}); failing loud per #22 acceptance"
        )
        raise EndpointMissingError(msg)
    return int(record["merged"]) if record["merged"] is not None else 0


def _missing_endpoint_ids(rows: list[dict[str, Any]]) -> list[str]:
    """Collect the distinct endpoint ids referenced by the edge rows.

    Used only to build the ``EndpointMissingError`` message / log line.
    The Cypher already determined *that* an endpoint was missing; without
    a second round-trip we cannot know exactly which ids were absent, so
    we surface every endpoint id in the failing batch (capped) as the
    operator's starting point for the bring-up audit. Bounded to keep the
    DLQ record and log line from ballooning on a large batch.
    """
    seen: list[str] = []
    for row in rows:
        for key in ("src_id", "dst_id"):
            value = row.get(key)
            if isinstance(value, str) and value not in seen:
                seen.append(value)
                if len(seen) >= 20:  # noqa: PLR2004 — log/message cap, not a domain constant
                    return seen
    return seen


# ---------------------------------------------------------------------------
# Processor
# ---------------------------------------------------------------------------


class IngestProcessor:
    """Terminal processor — manifest-gated MERGE into Neo4j."""

    def __init__(self, store: ObjectStore, neo4j_driver: Driver | None = None) -> None:
        self.store = store
        self.neo4j_driver = neo4j_driver

    def __call__(self, msg: PipelineMessage) -> None:
        prefix = _normalize_prefix(msg.b2_path)

        # Read manifest first so a missing ready signal surfaces before
        # we open a Neo4j session or fetch any Parquets.
        manifest = _read_manifest(self.store, prefix)
        node_entries, edges_entry = _split_manifest_entries(manifest["parquets"])

        # Fetch + validate every Parquet referenced by the manifest
        # before any Neo4j work. A missing Parquet must abort the batch
        # cleanly, never a partial load.
        nodes_by_label: dict[str, list[dict[str, Any]]] = {}
        total_nodes = 0
        for entry in node_entries:
            label = entry["label"]
            key = f"{prefix}{entry['path']}"
            try:
                with self.store.get_object(key) as stream:
                    raw_rows = _rows_from_parquet(stream)
            except Exception as exc:  # noqa: BLE001 — any read failure = missing parquet
                msg_err = f"manifest lists {label} parquet at {key!r} but read failed: {exc}"
                raise UnknownSchemaError(msg_err) from exc
            shaped = _node_rows_for_merge(label, raw_rows)
            if shaped:
                nodes_by_label[label] = shaped
                total_nodes += len(shaped)

        edges_by_label: dict[str, list[dict[str, Any]]] = {}
        if edges_entry is not None:
            key = f"{prefix}{edges_entry['path']}"
            try:
                with self.store.get_object(key) as stream:
                    raw_rows = _rows_from_parquet(stream)
            except Exception as exc:  # noqa: BLE001 — any read failure = missing parquet
                msg_err = f"manifest lists edges parquet at {key!r} but read failed: {exc}"
                raise UnknownSchemaError(msg_err) from exc
            edges_by_label = _edge_rows_for_merge(raw_rows)

        if not nodes_by_label and not edges_by_label:
            _logger.info(
                "ingest_empty_batch",
                batch_id=msg.batch_id,
                b2_path=msg.b2_path,
            )
            return None

        if self.neo4j_driver is None:
            # Read-path ergonomics for the unit suite — skip the write.
            _logger.warning(
                "ingest_no_driver",
                batch_id=msg.batch_id,
                b2_path=msg.b2_path,
            )
            return None

        def _run_batch(tx: ManagedTransaction) -> dict[str, dict[str, int]]:
            """Single-transaction closure — all nodes first, then edges.

            Running both phases inside one ``execute_write`` means the
            driver's retry covers the whole batch, and edges always see
            their endpoints MATCH-able because the MERGE of nodes ran in
            the same transaction scope. The driver retries all transient
            errors (``ServiceUnavailable``, ``TransientError``,
            ``SessionExpired``) internally, bounded by
            ``max_transaction_retry_time`` (default 30s); when the wall-
            clock budget is exhausted the last error propagates and the
            runner DLQs the message. Each retry iteration releases the
            connection back to the pool and reacquires a fresh one (see
            ``neo4j._sync.work.session._run_transaction`` lines 557 / 532),
            so a ``SessionExpired`` triggers a fresh-connection retry
            which on a cluster can land on a different cluster member.
            """
            merged_nodes: dict[str, int] = {}
            for node_label, label_rows in nodes_by_label.items():
                merged_nodes[node_label] = _merge_nodes_tx(tx, label=node_label, rows=label_rows)
            merged_edges: dict[str, int] = {}
            for edge_label, edge_rows in edges_by_label.items():
                merged_edges[edge_label] = _merge_edges_tx(tx, label=edge_label, rows=edge_rows)
            return {"nodes": merged_nodes, "edges": merged_edges}

        with self.neo4j_driver.session() as session:
            merged: dict[str, dict[str, int]] = session.execute_write(_run_batch)

        _logger.info(
            "ingest_merged",
            batch_id=msg.batch_id,
            b2_path=msg.b2_path,
            merged_nodes=merged["nodes"],
            merged_edges=merged["edges"],
            record_count=msg.record_count,
        )
        return None
