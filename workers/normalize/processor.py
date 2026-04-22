"""normalize-worker processor — fan out hadith rows into graph entities.

Per #192 Option D-ii the normalize stage writes *per-label* Parquet
files plus a ``_MANIFEST.json`` into a folder prefix keyed by
``batch_id``. Ingest (#18) reads the manifest to MERGE nodes in
node-first-then-edges order in a single Neo4j session.

Output layout::

    normalized/<batch_id>/
        narrators.parquet         # Narrator nodes
        hadiths.parquet           # Hadith nodes
        collections.parquet       # Collection nodes (deduped within batch)
        chains.parquet            # Chain nodes (one per hadith)
        gradings.parquet          # Grading nodes (only if grade present)
        edges.parquet             # all relationships
        _MANIFEST.json            # written LAST

Every per-label Parquet row has the shape ``{label, id, props}`` where
``props`` is a free-form dict carried through to Neo4j properties. The
edge Parquet has ``{label, src_id, src_label, dst_id, dst_label,
props}``. That shape is what ingest's ``_group_rows_by_label``
expects and matches the upstream batch loaders in
``src/graph/load_{nodes,edges}.py``.

Stable-ID generation
--------------------
IDs are deterministic so re-processing the same batch produces the same
node identities (idempotent ingest MERGE). Hash inputs:

* ``Hadith``   — ``hdt:<source_corpus>:<source_id>`` (already canonical
  from the source parser; no hashing required).
* ``Narrator`` — ``nar:<sha1-24>`` of
  ``<lower(name_en)>|<normalize_arabic(name_ar)>``. Blank sides are
  represented as the empty string so pure-English and pure-Arabic
  mentions still produce stable IDs.
* ``Chain``    — ``chn:<sha1-24>`` of
  ``<hadith_id>|<|-joined narrator_ids in chain order>``.
* ``Collection`` — ``col:<normalized_collection_name>`` (lowercase,
  spaces→underscores). Not hashed so the ID is human-readable and
  matches ``src/graph/load_nodes.py``.
* ``Grading`` — ``grd:<hadith_id>``; one grading node per hadith.

Phase-4 safety note (ingest side)
---------------------------------
Rows carry their properties inside a ``props`` dict rather than as
individual columns. Ingest's Cypher MUST translate this into enumerated
``SET n.key = row.props.key`` stanzas rather than ``SET n += row.props``
— Farhan flagged ``SET +=`` as unsafe because it lets an attacker-
controlled property name (``id``, ``:label``) subvert the node. See
#192 comment thread for the decision.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.parse.narrator_extraction import extract_narrator_mentions
from src.parse.schemas import HADITH_SCHEMA
from workers.lib.log import get_logger
from workers.lib.message import PipelineMessage
from workers.lib.object_store import ObjectStore
from workers.lib.topics import ALLOWED_NODE_LABELS

__all__ = ["NormalizeProcessor"]

_logger = get_logger("workers.normalize")

# Parquet filename for each node label. Labels absent from this map are
# not emitted by normalize; ingest will simply skip them (manifest lists
# only the files that were written).
_NODE_FILENAMES: dict[str, str] = {
    "Narrator": "narrators.parquet",
    "Hadith": "hadiths.parquet",
    "Collection": "collections.parquet",
    "Chain": "chains.parquet",
    "Grading": "gradings.parquet",
    # HistoricalEvent and Location are curated (YAML-sourced) rather than
    # extracted from hadith rows, so normalize doesn't emit them today.
    # They stay in ALLOWED_NODE_LABELS for the vocabulary contract with
    # ingest.
}

_EDGES_FILENAME = "edges.parquet"
_MANIFEST_FILENAME = "_MANIFEST.json"

# --------------------------------------------------------------------------
# Node + edge row shapes. Flat Arrow schemas let each per-label Parquet
# roundtrip cleanly through pyarrow without relying on struct/map types.
# --------------------------------------------------------------------------

_NODE_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("label", pa.string(), nullable=False),
        pa.field("id", pa.string(), nullable=False),
        # props is JSON-encoded so ingest can json.loads() it regardless
        # of which downstream props are present. Avoids an exhaustive
        # per-label Arrow schema for this early phase.
        pa.field("props", pa.string(), nullable=False),
    ]
)

_EDGE_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("label", pa.string(), nullable=False),
        pa.field("src_id", pa.string(), nullable=False),
        pa.field("src_label", pa.string(), nullable=False),
        pa.field("dst_id", pa.string(), nullable=False),
        pa.field("dst_label", pa.string(), nullable=False),
        pa.field("props", pa.string(), nullable=False),
    ]
)


@dataclass(frozen=True)
class _NodeRow:
    label: str
    id: str
    props: dict[str, Any]


@dataclass(frozen=True)
class _EdgeRow:
    label: str
    src_id: str
    src_label: str
    dst_id: str
    dst_label: str
    props: dict[str, Any]


# --------------------------------------------------------------------------
# ID + name helpers
# --------------------------------------------------------------------------


def _sha1_24(canonical: str) -> str:
    """24-hex-char truncation of SHA-1. Enough entropy for node identity,
    short enough for readable Neo4j ids. Matches the convention in
    ``src/resolve/`` for narrator canonical ids.
    """
    return hashlib.sha1(canonical.encode("utf-8"), usedforsecurity=False).hexdigest()[:24]


def _normalize_collection_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")


def _narrator_id(name_en: str | None, name_ar: str | None) -> str:
    """Stable narrator id from name fields.

    Either side may be missing; the hash input keeps both in canonical
    order so a later pass that fills in the Arabic side still produces
    the same id from an identical English input when no Arabic was known.
    """
    en = (name_en or "").strip().lower()
    ar_norm = _normalize_arabic_safe(name_ar) if name_ar else ""
    return f"nar:{_sha1_24(f'en:{en}|ar:{ar_norm}')}"


def _normalize_arabic_safe(text: str) -> str:
    """Wrap ``src.utils.arabic.normalize_arabic`` but fall back cleanly."""
    try:
        from src.utils.arabic import normalize_arabic

        return normalize_arabic(text).strip()
    except Exception:  # noqa: BLE001 — normalization is best-effort for hashing
        return text.strip()


def _hadith_id(source_corpus: str, source_id: str) -> str:
    key = f"{source_corpus}:{source_id}"
    return key if key.startswith("hdt:") else f"hdt:{key}"


def _collection_id(collection_name: str) -> str:
    return f"col:{_normalize_collection_name(collection_name)}"


def _chain_id(hadith_id: str, narrator_ids: list[str]) -> str:
    return f"chn:{_sha1_24(hadith_id + '|' + '|'.join(narrator_ids))}"


def _grading_id(hadith_id: str) -> str:
    return f"grd:{hadith_id}"


# --------------------------------------------------------------------------
# Fan-out core
# --------------------------------------------------------------------------


def _fan_out_row(row: dict[str, Any]) -> tuple[list[_NodeRow], list[_EdgeRow]]:
    """Expand a single HADITH_SCHEMA row into nodes + edges."""
    source_id = row["source_id"]
    source_corpus = row["source_corpus"]
    collection_name = row["collection_name"]
    sect = row["sect"]

    hid = _hadith_id(source_corpus, source_id)
    cid = _collection_id(collection_name)

    nodes: list[_NodeRow] = [
        _NodeRow(
            label="Hadith",
            id=hid,
            props={
                "matn_ar": row.get("matn_ar"),
                "matn_en": row.get("matn_en"),
                "isnad_raw_ar": row.get("isnad_raw_ar"),
                "isnad_raw_en": row.get("isnad_raw_en"),
                "grade": row.get("grade"),
                "source_corpus": source_corpus,
                "sect": sect,
                "collection_name": collection_name,
                "book_number": row.get("book_number"),
                "chapter_number": row.get("chapter_number"),
                "hadith_number": row.get("hadith_number"),
                "chapter_name_ar": row.get("chapter_name_ar"),
                "chapter_name_en": row.get("chapter_name_en"),
            },
        ),
        _NodeRow(
            label="Collection",
            id=cid,
            props={
                "name_en": collection_name,
                "sect": sect,
                "source_corpus": source_corpus,
            },
        ),
    ]

    edges: list[_EdgeRow] = [
        _EdgeRow(
            label="APPEARS_IN",
            src_id=hid,
            src_label="Hadith",
            dst_id=cid,
            dst_label="Collection",
            props={
                "book_number": row.get("book_number"),
                "chapter_number": row.get("chapter_number"),
                "hadith_number": row.get("hadith_number"),
            },
        ),
    ]

    # ---- Narrators + Chain + TRANSMITTED_TO + NARRATED ----
    isnad = row.get("isnad_raw_en") or row.get("isnad_raw_ar") or ""
    language = "en" if row.get("isnad_raw_en") else "ar"
    narrator_ids: list[str] = []

    if isnad:
        mentions = extract_narrator_mentions(isnad, language)
        for mention in mentions:
            if language == "en":
                nid = _narrator_id(mention.name, None)
                name_en: str | None = mention.name
                name_ar: str | None = None
            else:
                nid = _narrator_id(None, mention.name)
                name_en = None
                name_ar = mention.name
            nodes.append(
                _NodeRow(
                    label="Narrator",
                    id=nid,
                    props={
                        "name_en": name_en,
                        "name_ar": name_ar,
                        "name_ar_normalized": (
                            _normalize_arabic_safe(name_ar) if name_ar else None
                        ),
                        "transmission_method": mention.transmission_method,
                    },
                )
            )
            narrator_ids.append(nid)

        # TRANSMITTED_TO chain pairs — matches src/graph/load_edges.py
        for idx in range(len(narrator_ids) - 1):
            edges.append(
                _EdgeRow(
                    label="TRANSMITTED_TO",
                    src_id=narrator_ids[idx],
                    src_label="Narrator",
                    dst_id=narrator_ids[idx + 1],
                    dst_label="Narrator",
                    props={
                        "position_in_chain": idx,
                        "hadith_id": hid,
                    },
                )
            )

        # NARRATED: first (earliest) narrator -> hadith
        if narrator_ids:
            edges.append(
                _EdgeRow(
                    label="NARRATED",
                    src_id=narrator_ids[0],
                    src_label="Narrator",
                    dst_id=hid,
                    dst_label="Hadith",
                    props={},
                )
            )

    # ---- Chain node (always, even for empty isnad — matches load_nodes) ----
    chn_id = _chain_id(hid, narrator_ids)
    nodes.append(
        _NodeRow(
            label="Chain",
            id=chn_id,
            props={
                "hadith_id": hid,
                "chain_index": 0,
                "chain_length": len(narrator_ids),
                "is_complete": len(narrator_ids) > 0,
                "narrator_ids": narrator_ids,
            },
        )
    )

    # ---- Grading + GRADED_BY (iff grade present) ----
    grade = row.get("grade")
    if grade:
        grading_id = _grading_id(hid)
        nodes.append(
            _NodeRow(
                label="Grading",
                id=grading_id,
                props={
                    "hadith_id": hid,
                    "grade": grade,
                    "scholar_name": collection_name,
                },
            )
        )
        edges.append(
            _EdgeRow(
                label="GRADED_BY",
                src_id=hid,
                src_label="Hadith",
                dst_id=grading_id,
                dst_label="Grading",
                props={},
            )
        )

    return nodes, edges


# --------------------------------------------------------------------------
# Parquet serialisation
# --------------------------------------------------------------------------


def _nodes_to_parquet(label: str, nodes: list[_NodeRow]) -> bytes:
    if not nodes:
        table = _NODE_SCHEMA.empty_table()
    else:
        table = pa.table(
            {
                "label": [label] * len(nodes),
                "id": [n.id for n in nodes],
                "props": [json.dumps(n.props, ensure_ascii=False, default=str) for n in nodes],
            },
            schema=_NODE_SCHEMA,
        )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _edges_to_parquet(edges: list[_EdgeRow]) -> bytes:
    if not edges:
        table = _EDGE_SCHEMA.empty_table()
    else:
        table = pa.table(
            {
                "label": [e.label for e in edges],
                "src_id": [e.src_id for e in edges],
                "src_label": [e.src_label for e in edges],
                "dst_id": [e.dst_id for e in edges],
                "dst_label": [e.dst_label for e in edges],
                "props": [json.dumps(e.props, ensure_ascii=False, default=str) for e in edges],
            },
            schema=_EDGE_SCHEMA,
        )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


# --------------------------------------------------------------------------
# Normalize + fan-out processor
# --------------------------------------------------------------------------


def _required_fields(schema: pa.Schema) -> list[str]:
    return [f.name for f in schema if not f.nullable]


class NormalizeProcessor:
    """Fan out a hadith-batch Parquet into per-label node + edge Parquets."""

    def __init__(self, store: ObjectStore, target_schema: pa.Schema = HADITH_SCHEMA) -> None:
        self.store = store
        self.target_schema = target_schema
        self._required = _required_fields(target_schema)

    # ---- shape coercion (unchanged behaviour, reused from the prior impl) ----

    def _coerce_to_target_schema(self, payload: bytes) -> tuple[pa.Table, int]:
        """Return (normalized_table, dropped_row_count). Raises on unusable input."""
        table = pq.read_table(io.BytesIO(payload))

        missing = [f.name for f in self.target_schema if f.name not in table.column_names]
        if missing:
            raise ValueError(f"input parquet missing required columns: {missing}")

        projected = table.select([f.name for f in self.target_schema])

        dropped = 0
        if self._required:
            mask = pa.array([True] * projected.num_rows, type=pa.bool_())
            for name in self._required:
                mask = pc.and_(mask, pc.is_valid(projected.column(name)))
            kept = projected.filter(mask)
            dropped = projected.num_rows - kept.num_rows
            projected = kept

        try:
            normalized = projected.cast(self.target_schema, safe=False)
        except pa.ArrowInvalid as exc:
            raise ValueError(f"cannot cast batch to HADITH_SCHEMA: {exc}") from exc

        return normalized, dropped

    # ---- atomic write helpers ----

    def _write_atomic(self, key: str, data: bytes, content_type: str) -> None:
        """Write ``data`` to a ``.part`` suffix then rename to ``key``.

        The suffix is randomised so two concurrent retries for the same
        key can't clobber each other's staging object. Rename is atomic
        server-side (S3 ``CopyObject`` + ``DeleteObject``), so readers
        never see a half-written final key.
        """
        part_key = f"{key}.part.{uuid.uuid4().hex[:12]}"
        self.store.put_object(part_key, data, content_type=content_type)
        self.store.rename_object(part_key, key)

    # ---- the main entry point ----

    def __call__(self, msg: PipelineMessage) -> PipelineMessage:
        payload = self.store.get_object(msg.b2_path)

        try:
            normalized, dropped = self._coerce_to_target_schema(payload)
        except (pa.ArrowInvalid, ValueError) as exc:
            _logger.error("normalize_invalid_batch", batch_id=msg.batch_id, error=str(exc))
            raise

        # Fan-out every surviving row into graph entities.
        nodes_by_label: dict[str, list[_NodeRow]] = {}
        all_edges: list[_EdgeRow] = []
        for row in normalized.to_pylist():
            nodes, edges = _fan_out_row(row)
            for node in nodes:
                if node.label not in ALLOWED_NODE_LABELS:
                    raise ValueError(f"fan-out produced unknown node label {node.label!r}")
                nodes_by_label.setdefault(node.label, []).append(node)
            all_edges.extend(edges)

        # Dedupe nodes within the batch by id — collections, narrators,
        # and chains can repeat across hadith rows and ingest is already
        # idempotent via MERGE, but shipping duplicates bloats the Parquet
        # and wastes Cypher round-trips.
        for label, rows in nodes_by_label.items():
            seen: dict[str, _NodeRow] = {}
            for node in rows:
                # First-writer-wins; later duplicates are expected to
                # carry the same props, so dropping them is safe.
                seen.setdefault(node.id, node)
            nodes_by_label[label] = list(seen.values())

        # ---- write per-label Parquets under a .part suffix, then rename ----
        prefix = f"normalized/{msg.batch_id}/"
        manifest_entries: list[dict[str, Any]] = []
        total_row_count = 0

        for label, rows in nodes_by_label.items():
            if not rows:
                continue
            filename = _NODE_FILENAMES[label]
            key = f"{prefix}{filename}"
            data = _nodes_to_parquet(label, rows)
            self._write_atomic(key, data, content_type="application/octet-stream")
            manifest_entries.append(
                {
                    "path": filename,
                    "label": label,
                    "row_count": len(rows),
                }
            )
            total_row_count += len(rows)

        # edges parquet — always written (may be empty)
        edges_key = f"{prefix}{_EDGES_FILENAME}"
        edges_data = _edges_to_parquet(all_edges)
        self._write_atomic(edges_key, edges_data, content_type="application/octet-stream")
        manifest_entries.append(
            {
                "path": _EDGES_FILENAME,
                "row_count": len(all_edges),
            }
        )
        total_row_count += len(all_edges)

        # ---- manifest LAST (ingest uses this as the ready signal) ----
        manifest = {
            "batch_id": msg.batch_id,
            "source": msg.source,
            "created_at": datetime.now(UTC).isoformat(),
            "parquets": manifest_entries,
            "total_row_count": total_row_count,
        }
        self.store.put_object(
            f"{prefix}{_MANIFEST_FILENAME}",
            json.dumps(manifest, ensure_ascii=False).encode("utf-8"),
            content_type="application/json",
        )

        _logger.info(
            "normalize_complete",
            batch_id=msg.batch_id,
            rows_in=normalized.num_rows,
            rows_dropped=dropped,
            nodes_by_label={k: len(v) for k, v in nodes_by_label.items()},
            edges=len(all_edges),
        )

        return msg.to_next_stage(b2_path=prefix, record_count=total_row_count)
