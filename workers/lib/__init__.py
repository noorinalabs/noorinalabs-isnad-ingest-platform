"""Shared library for streaming pipeline workers.

Exposes the pointer-message schema, Kafka consumer/producer helpers,
S3 object store client, DLQ producer, and metrics emitter. Each worker
imports from here rather than duplicating infrastructure code.
"""

from workers.lib.message import PipelineMessage, parse_message, serialize_message
from workers.lib.runner import WorkerRunner, WorkerSettings

__all__ = [
    "PipelineMessage",
    "WorkerRunner",
    "WorkerSettings",
    "parse_message",
    "serialize_message",
]
