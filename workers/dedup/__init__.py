"""dedup-worker.

Consumes ``pipeline.raw.new``. For each batch the worker pulls the raw
Parquet from ``raw/{source}/{YYYY-MM-DD}/``, runs hadith deduplication
(ports ``src/resolve/dedup.py``), and writes the deduped Parquet to
``dedup/{source}/{batch_id}/``. Emits ``pipeline.dedup.done`` on success.

Scope: this scaffold implements the happy path — load batch, call the
dedup entry point, write output, forward pointer. Full logic port (FAISS
index caching, cross-corpus linking, classify-pair tiers) is tracked as
TODOs in the per-stage processor module.
"""
