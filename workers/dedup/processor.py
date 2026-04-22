"""dedup-worker processor — port of ``src/resolve/dedup.py`` for streaming.

Ported behaviour (per-batch):

1. Fetch input Parquet at ``msg.b2_path`` (schema: ``HADITH_SCHEMA``).
2. Embed ``matn_en`` with a sentence-transformers model.
3. Build an in-batch FAISS IndexFlatIP over the normalized embeddings.
4. Retrieve top-K neighbours, filter by threshold, classify via variant
   tiering (VERBATIM ≥ 0.90, CLOSE_PARAPHRASE ≥ 0.80, else THEMATIC),
   and flag cross-sect pairs.
5. Emit ``dedup/{source}/{batch_id}/parallel_links.parquet`` plus the
   pass-through ``hadiths.parquet`` so downstream stages have a payload.

Known scope limitation vs upstream batch job
--------------------------------------------
The upstream ``run_dedup`` operates over a staging directory spanning
multiple source corpora and thus detects cross-batch/cross-source
duplicates. A Kafka worker sees one batch at a time, so only
*intra-batch* pairs are emitted here. Cross-batch reconciliation is a
post-pipeline concern and is tracked outside this issue.

Graceful degradation
--------------------
Missing optional ML dependencies (``sentence-transformers``, ``faiss``,
``numpy``) do not fail the message. We log and emit an empty
``parallel_links.parquet`` matching ``PARALLEL_LINKS_SCHEMA``, same as
the upstream's behaviour.

Stateful components (embedding model, FAISS class) are lazy-initialised
at first-message time and cached on the processor instance so later
messages reuse them.
"""

from __future__ import annotations

import io
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from src.models.enums import VariantType
from src.resolve.schemas import PARALLEL_LINKS_SCHEMA
from workers.lib.log import get_logger
from workers.lib.message import PipelineMessage
from workers.lib.object_store import ObjectStore

__all__ = ["DedupProcessor"]

_logger = get_logger("workers.dedup")

_SUNNI_SOURCES: frozenset[str] = frozenset({"lk", "sanadset", "sunnah", "fawaz", "open_hadith"})
_SHIA_SOURCES: frozenset[str] = frozenset({"thaqalayn"})

_EMBED_MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
_DEFAULT_BATCH_SIZE = 256
_DEFAULT_TOP_K = 50
_DEFAULT_THRESHOLD = 0.70


def _classify_pair(score: float) -> VariantType:
    if score >= 0.90:
        return VariantType.VERBATIM
    if score >= 0.80:
        return VariantType.CLOSE_PARAPHRASE
    return VariantType.THEMATIC


def _is_cross_sect(corpus_a: str, corpus_b: str) -> bool:
    a_sunni = corpus_a in _SUNNI_SOURCES
    b_sunni = corpus_b in _SUNNI_SOURCES
    a_shia = corpus_a in _SHIA_SOURCES
    b_shia = corpus_b in _SHIA_SOURCES
    return (a_sunni and b_shia) or (a_shia and b_sunni)


def _empty_links_bytes() -> bytes:
    """Serialize an empty PARALLEL_LINKS_SCHEMA table to Parquet bytes."""
    buf = io.BytesIO()
    pq.write_table(PARALLEL_LINKS_SCHEMA.empty_table(), buf)
    return buf.getvalue()


class DedupProcessor:
    """Callable processor: dedup a single batch and emit parallel-link pairs."""

    def __init__(
        self,
        store: ObjectStore,
        *,
        top_k: int = _DEFAULT_TOP_K,
        threshold: float = _DEFAULT_THRESHOLD,
        embed_batch_size: int = _DEFAULT_BATCH_SIZE,
    ) -> None:
        self.store = store
        self.top_k = top_k
        self.threshold = threshold
        self.embed_batch_size = embed_batch_size
        self._model: Any | None = None
        self._faiss: Any | None = None
        self._np: Any | None = None
        self._ml_unavailable = False

    def _ensure_ml(self) -> bool:
        """Lazy-init ML deps. Returns False if any dep is missing."""
        if self._ml_unavailable:
            return False
        if self._model is not None and self._faiss is not None and self._np is not None:
            return True
        try:
            import faiss
            import numpy as np
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            _logger.warning(
                "dedup_ml_deps_unavailable",
                error=str(exc),
                hint="install the 'ml' dependency group for real dedup",
            )
            self._ml_unavailable = True
            return False
        if self._model is None:
            _logger.info("dedup_loading_model", model=_EMBED_MODEL_NAME)
            self._model = SentenceTransformer(_EMBED_MODEL_NAME)
        self._faiss = faiss
        self._np = np
        return True

    def _load_batch(self, payload: bytes) -> tuple[list[str], list[str], list[str]]:
        """Read the input Parquet and return (ids, texts, corpora), dropping null matn rows."""
        table = pq.read_table(
            io.BytesIO(payload), columns=["source_id", "matn_en", "source_corpus"]
        )
        ids: list[str] = []
        texts: list[str] = []
        corpora: list[str] = []
        skipped = 0
        for i in range(table.num_rows):
            matn = table.column("matn_en")[i].as_py()
            if not matn or not matn.strip():
                skipped += 1
                continue
            ids.append(table.column("source_id")[i].as_py())
            texts.append(matn)
            corpora.append(table.column("source_corpus")[i].as_py())
        if skipped:
            _logger.info("dedup_skipped_null_matn", skipped=skipped, kept=len(ids))
        return ids, texts, corpora

    def _embed(self, texts: list[str]) -> Any:
        assert self._model is not None
        assert self._np is not None
        np_ = self._np
        chunks: list[Any] = []
        for start in range(0, len(texts), self.embed_batch_size):
            end = min(start + self.embed_batch_size, len(texts))
            chunk = self._model.encode(
                texts[start:end],
                batch_size=self.embed_batch_size,
                show_progress_bar=False,
                convert_to_numpy=True,
                normalize_embeddings=True,
            )
            chunks.append(chunk)
        return np_.ascontiguousarray(np_.vstack(chunks), dtype=np_.float32)

    def _find_pairs(
        self,
        hadith_ids: list[str],
        corpora: list[str],
        embeddings: Any,
    ) -> pa.Table:
        """Run FAISS top-K search and collect pairs above threshold."""
        assert self._faiss is not None
        dim = embeddings.shape[1]
        index = self._faiss.IndexFlatIP(dim)
        index.add(embeddings)
        actual_k = min(self.top_k + 1, len(hadith_ids))
        scores, indices = index.search(embeddings, actual_k)

        id_to_corpus = dict(zip(hadith_ids, corpora))
        seen_pairs: set[tuple[str, str]] = set()
        ids_a: list[str] = []
        ids_b: list[str] = []
        sim_scores: list[float] = []
        variant_types: list[str] = []
        cross_sects: list[bool] = []

        for i in range(len(hadith_ids)):
            hid_a = hadith_ids[i]
            for j_idx in range(actual_k):
                neighbor = int(indices[i, j_idx])
                score = float(scores[i, j_idx])
                if neighbor < 0 or neighbor == i or score < self.threshold:
                    continue
                hid_b = hadith_ids[neighbor]
                pair_key = (hid_b, hid_a) if hid_a >= hid_b else (hid_a, hid_b)
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)
                ids_a.append(pair_key[0])
                ids_b.append(pair_key[1])
                sim_scores.append(score)
                variant_types.append(str(_classify_pair(score)))
                cross_sects.append(
                    _is_cross_sect(id_to_corpus[pair_key[0]], id_to_corpus[pair_key[1]])
                )

        return pa.table(
            {
                "hadith_id_a": pa.array(ids_a, type=pa.string()),
                "hadith_id_b": pa.array(ids_b, type=pa.string()),
                "similarity_score": pa.array(sim_scores, type=pa.float32()),
                "variant_type": pa.array(variant_types, type=pa.string()),
                "cross_sect": pa.array(cross_sects, type=pa.bool_()),
            },
            schema=PARALLEL_LINKS_SCHEMA,
        )

    def __call__(self, msg: PipelineMessage) -> PipelineMessage:
        payload = self.store.get_object(msg.b2_path)

        out_prefix = f"dedup/{msg.source}/{msg.batch_id}"
        hadiths_key = f"{out_prefix}/hadiths.parquet"
        links_key = f"{out_prefix}/parallel_links.parquet"

        # Pass-through the hadith payload unchanged so downstream stages
        # consume from the dedup prefix regardless of dedup success.
        self.store.put_object(hadiths_key, payload)

        if not self._ensure_ml():
            self.store.put_object(links_key, _empty_links_bytes())
            return msg.to_next_stage(b2_path=hadiths_key)

        try:
            ids, texts, corpora = self._load_batch(payload)
        except pa.ArrowInvalid as exc:
            # Malformed Parquet → raise so the runner routes to DLQ.
            _logger.error("dedup_bad_parquet", batch_id=msg.batch_id, error=str(exc))
            raise

        if not texts:
            _logger.info("dedup_no_texts", batch_id=msg.batch_id)
            self.store.put_object(links_key, _empty_links_bytes())
            return msg.to_next_stage(b2_path=hadiths_key)

        embeddings = self._embed(texts)
        links_table = self._find_pairs(ids, corpora, embeddings)

        buf = io.BytesIO()
        pq.write_table(links_table, buf)
        self.store.put_object(links_key, buf.getvalue())

        _logger.info(
            "dedup_complete",
            batch_id=msg.batch_id,
            hadiths=len(ids),
            pairs=links_table.num_rows,
        )
        return msg.to_next_stage(b2_path=hadiths_key)
