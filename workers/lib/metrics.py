"""Worker metrics emitter.

Minimal counter/timer facade. Default implementation logs structured
events via ``structlog``; production will swap this for a Prometheus
client or a StatsD emitter via dependency injection.

Metrics emitted per batch:

- ``records_processed`` — count of input records
- ``duration_seconds`` — wall clock for the batch
- ``errors`` — count of failed batches (DLQ routes)
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager

from workers.lib.log import get_logger

__all__ = ["Metrics", "time_batch"]

_logger = get_logger("workers.metrics")


class Metrics:
    """Structured-log metrics emitter. Swap for Prometheus in prod."""

    def __init__(self, worker: str) -> None:
        self.worker = worker

    def records_processed(self, count: int, *, batch_id: str) -> None:
        _logger.info("metric_records_processed", worker=self.worker, batch_id=batch_id, count=count)

    def duration_seconds(self, seconds: float, *, batch_id: str) -> None:
        _logger.info(
            "metric_duration_seconds", worker=self.worker, batch_id=batch_id, seconds=seconds
        )

    def errors(self, *, batch_id: str, error_class: str) -> None:
        _logger.warning(
            "metric_error", worker=self.worker, batch_id=batch_id, error_class=error_class
        )


@contextmanager
def time_batch(metrics: Metrics, *, batch_id: str) -> Iterator[None]:
    """Context manager — emit a ``duration_seconds`` metric on exit."""
    start = time.monotonic()
    try:
        yield
    finally:
        metrics.duration_seconds(time.monotonic() - start, batch_id=batch_id)
