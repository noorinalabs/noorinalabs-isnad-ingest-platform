"""FastAPI dependencies for the reset HTTP surface.

The resetter and data dir are provided as dependencies so the unit suite can
override them with in-memory fakes (``app.dependency_overrides``) without any
boto3/Kafka/Neo4j/PG infra — the same isolation invariant the CLI keeps via
``_build_reset_clients``.
"""

from __future__ import annotations

from pathlib import Path

from src.config import get_settings
from src.pipeline.reset import PipelineResetter


def get_data_dir() -> Path:
    """Directory under which audit entries are written (``<data>/audit/``).

    Matches the CLI's ``_cmd_reset`` derivation so API-driven and CLI-driven
    resets land their audit log in the same place.
    """
    return Path(get_settings().data_raw_dir).parent


def get_resetter() -> PipelineResetter:
    """Build the real PipelineResetter wired to live infra adapters.

    Reuses the CLI's ``_build_reset_clients`` so there is a single place that
    constructs the boto3 / Kafka / Neo4j / PG adapters. Tests override this
    dependency with a fake resetter.
    """
    from src.cli import _build_reset_clients

    settings = get_settings()
    object_store, kafka_admin, neo4j, pg = _build_reset_clients(settings)
    return PipelineResetter(
        object_store=object_store,
        kafka_admin=kafka_admin,
        neo4j=neo4j,
        pg=pg,
        data_dir=get_data_dir(),
    )
