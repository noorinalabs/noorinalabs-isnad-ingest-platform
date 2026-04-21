"""Structured logging for streaming workers.

Kept independent of ``src/utils/logging.py`` because ``src/utils/__init__.py``
eagerly imports Neo4j and Postgres clients that streaming workers don't
need. Mirrors the same structlog-based output format (service tag, ISO
timestamp, JSON or console rendering by ``LOG_FORMAT``).
"""

from __future__ import annotations

import logging
import os
from typing import Any

import structlog

SERVICE_NAME = "isnad-graph-ingestion-workers"

__all__ = ["SERVICE_NAME", "configure_logging", "get_logger"]

_configured = False


def _add_service_name(
    _logger: Any, _method_name: str, event_dict: structlog.types.EventDict
) -> structlog.types.EventDict:
    event_dict.setdefault("service", SERVICE_NAME)
    return event_dict


def configure_logging() -> None:
    """Idempotent structlog configuration. Safe to call multiple times."""
    global _configured
    if _configured:
        return

    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    log_format = os.environ.get("LOG_FORMAT", "console").lower()

    logging.basicConfig(format="%(message)s", level=getattr(logging, log_level, logging.INFO))

    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _add_service_name,
    ]
    if log_format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level, logging.INFO)
        ),
        cache_logger_on_first_use=True,
    )
    _configured = True


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Fetch a bound logger. Auto-configures on first call."""
    if not _configured:
        configure_logging()
    return structlog.get_logger(name)
