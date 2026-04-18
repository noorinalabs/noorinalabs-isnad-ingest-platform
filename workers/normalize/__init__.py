"""normalize-worker.

Consumes ``pipeline.enrich.done``, unifies per-source schemas into a
single canonical shape, writes to ``normalized/{batch_id}/``, and
publishes ``pipeline.norm.done``.

This worker has no batch-pipeline predecessor — the normalization step
is new for the streaming pipeline.
"""
