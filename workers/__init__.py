"""Streaming Kafka workers for the ingestion pipeline.

Each worker consumes from one topic, processes a batch (pointer-based:
the Kafka message carries a B2/S3 object path), writes results back to
S3-compatible storage, and publishes to the next topic. Failures route
to ``pipeline.dlq``.

Workers are designed to be idempotent on ``batch_id``.
"""
