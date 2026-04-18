"""enrich-worker.

Consumes ``pipeline.dedup.done``, runs graph metrics, topic
classification, and historical linking (ports ``src/enrich/``), writes
to ``enriched/{source}/{batch_id}/``, and publishes
``pipeline.enrich.done``.
"""
