"""enrich-worker processor — per-batch topic classification.

What this stage does
--------------------
Reads the dedup-stage Parquet, runs zero-shot topic classification on
``matn_en`` using ``facebook/bart-large-mnli`` (mirrors
``src/enrich/topics.py``), and emits:

* ``enriched/{source}/{batch_id}/hadiths.parquet`` — pass-through of the
  input payload so downstream stages continue to see the hadith rows.
* ``enriched/{source}/{batch_id}/hadith_topics.parquet`` — a new side
  table with ``(source_id, topic_1, topic_1_score, topic_2,
  topic_2_score, topic_3, topic_3_score)`` that the ingest stage can
  read alongside the hadith rows when MERGEing into Neo4j.

Scope note — graph-dependent enrichments deferred
--------------------------------------------------
``src/enrich/metrics.py`` (Neo4j GDS centrality / PageRank / Louvain)
and ``src/enrich/historical.py`` (ACTIVE_DURING edges by date overlap)
both read and write the Neo4j graph after nodes and edges already
exist. They cannot run before ingest. In the streaming topology they
belong to a post-ingest enrichment stage (tracked as a follow-up);
only the payload-only classifier is ported here.

Graceful degradation
--------------------
Missing ``transformers`` / ``torch``, model-download failures, or a
single-batch classifier error log and emit an empty topics side table
rather than fail the message.
"""

from __future__ import annotations

import io
import os
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from workers.lib.log import get_logger
from workers.lib.message import PipelineMessage
from workers.lib.object_store import ObjectStore

__all__ = ["EnrichProcessor"]

_logger = get_logger("workers.enrich")

_MODEL_NAME = "facebook/bart-large-mnli"
_BATCH_SIZE = 32
_MIN_TEXT_LENGTH = 20

TOPIC_LABELS: tuple[str, ...] = (
    "theology",
    "jurisprudence",
    "eschatology",
    "succession/imamate",
    "ritual/worship",
    "ethics/conduct",
    "history/sira",
    "commerce/trade",
    "warfare/jihad",
    "family_law",
    "food/drink",
    "medicine",
    "dreams/visions",
    "end_times",
)

HADITH_TOPICS_SCHEMA = pa.schema(
    [
        pa.field("source_id", pa.string(), nullable=False),
        pa.field("topic_1", pa.string(), nullable=True),
        pa.field("topic_1_score", pa.float32(), nullable=True),
        pa.field("topic_2", pa.string(), nullable=True),
        pa.field("topic_2_score", pa.float32(), nullable=True),
        pa.field("topic_3", pa.string(), nullable=True),
        pa.field("topic_3_score", pa.float32(), nullable=True),
    ]
)


def _empty_topics_bytes() -> bytes:
    buf = io.BytesIO()
    pq.write_table(HADITH_TOPICS_SCHEMA.empty_table(), buf)
    return buf.getvalue()


class EnrichProcessor:
    """Callable processor: classify topics per hadith in the batch."""

    def __init__(
        self,
        store: ObjectStore,
        *,
        labels: tuple[str, ...] = TOPIC_LABELS,
        batch_size: int = _BATCH_SIZE,
    ) -> None:
        self.store = store
        self.labels = list(labels)
        self.batch_size = batch_size
        self._classifier: Any | None = None
        self._ml_unavailable = False

    def _ensure_classifier(self) -> bool:
        if self._ml_unavailable:
            return False
        if self._classifier is not None:
            return True
        try:
            from transformers import pipeline
        except ImportError as exc:
            _logger.warning(
                "enrich_transformers_unavailable",
                error=str(exc),
                hint="install the 'ml' dependency group for real topic classification",
            )
            self._ml_unavailable = True
            return False

        offline = os.environ.get("HF_HUB_OFFLINE", "0") == "1"
        kwargs: dict[str, Any] = {"model": _MODEL_NAME, "device": -1}
        if offline:
            kwargs["local_files_only"] = True
        try:
            self._classifier = pipeline("zero-shot-classification", **kwargs)
        except Exception as exc:  # noqa: BLE001 — includes HF network/hub errors
            _logger.error("enrich_pipeline_load_failed", model=_MODEL_NAME, error=str(exc))
            self._ml_unavailable = True
            return False
        _logger.info("enrich_pipeline_loaded", model=_MODEL_NAME)
        return True

    def _load_batch(self, payload: bytes) -> tuple[list[str], list[str]]:
        """Read the input Parquet and return (ids, texts), dropping too-short matn rows."""
        table = pq.read_table(io.BytesIO(payload), columns=["source_id", "matn_en"])
        ids: list[str] = []
        texts: list[str] = []
        for i in range(table.num_rows):
            matn = table.column("matn_en")[i].as_py()
            if not matn or len(matn) < _MIN_TEXT_LENGTH:
                continue
            ids.append(table.column("source_id")[i].as_py())
            texts.append(matn)
        return ids, texts

    def _classify_rows(self, ids: list[str], texts: list[str]) -> list[dict[str, Any]]:
        assert self._classifier is not None
        rows: list[dict[str, Any]] = []
        for start in range(0, len(texts), self.batch_size):
            end = min(start + self.batch_size, len(texts))
            chunk_texts = texts[start:end]
            chunk_ids = ids[start:end]
            try:
                results = self._classifier(
                    chunk_texts, candidate_labels=self.labels, multi_label=False
                )
            except Exception as exc:  # noqa: BLE001
                _logger.error("enrich_batch_failed", offset=start, error=str(exc))
                continue
            if isinstance(results, dict):
                results = [results]
            for hid, result in zip(chunk_ids, results):
                labels = result["labels"]
                scores = result["scores"]
                rows.append(
                    {
                        "source_id": hid,
                        "topic_1": labels[0],
                        "topic_1_score": round(float(scores[0]), 4),
                        "topic_2": labels[1] if len(labels) > 1 else None,
                        "topic_2_score": (round(float(scores[1]), 4) if len(scores) > 1 else None),
                        "topic_3": labels[2] if len(labels) > 2 else None,
                        "topic_3_score": (round(float(scores[2]), 4) if len(scores) > 2 else None),
                    }
                )
        return rows

    def _build_topics_table(self, rows: list[dict[str, Any]]) -> pa.Table:
        if not rows:
            return HADITH_TOPICS_SCHEMA.empty_table()
        return pa.table(
            {
                "source_id": pa.array([r["source_id"] for r in rows], type=pa.string()),
                "topic_1": pa.array([r["topic_1"] for r in rows], type=pa.string()),
                "topic_1_score": pa.array([r["topic_1_score"] for r in rows], type=pa.float32()),
                "topic_2": pa.array([r["topic_2"] for r in rows], type=pa.string()),
                "topic_2_score": pa.array([r["topic_2_score"] for r in rows], type=pa.float32()),
                "topic_3": pa.array([r["topic_3"] for r in rows], type=pa.string()),
                "topic_3_score": pa.array([r["topic_3_score"] for r in rows], type=pa.float32()),
            },
            schema=HADITH_TOPICS_SCHEMA,
        )

    def __call__(self, msg: PipelineMessage) -> PipelineMessage:
        payload = self.store.get_object(msg.b2_path)

        out_prefix = f"enriched/{msg.source}/{msg.batch_id}"
        hadiths_key = f"{out_prefix}/hadiths.parquet"
        topics_key = f"{out_prefix}/hadith_topics.parquet"

        # Always pass-through the hadith payload so downstream stages
        # see the full set of rows regardless of classifier availability.
        self.store.put_object(hadiths_key, payload)

        if not self._ensure_classifier():
            self.store.put_object(topics_key, _empty_topics_bytes())
            return msg.to_next_stage(b2_path=hadiths_key)

        try:
            ids, texts = self._load_batch(payload)
        except pa.ArrowInvalid as exc:
            _logger.error("enrich_bad_parquet", batch_id=msg.batch_id, error=str(exc))
            raise

        if not texts:
            _logger.info("enrich_no_texts", batch_id=msg.batch_id)
            self.store.put_object(topics_key, _empty_topics_bytes())
            return msg.to_next_stage(b2_path=hadiths_key)

        rows = self._classify_rows(ids, texts)
        topics_table = self._build_topics_table(rows)

        buf = io.BytesIO()
        pq.write_table(topics_table, buf)
        self.store.put_object(topics_key, buf.getvalue())

        _logger.info(
            "enrich_complete",
            batch_id=msg.batch_id,
            hadiths=len(ids),
            classified=topics_table.num_rows,
        )
        return msg.to_next_stage(b2_path=hadiths_key)
