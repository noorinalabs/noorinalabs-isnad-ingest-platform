"""FastAPI dependencies for the reset HTTP surface.

The resetter is provided as a *lazy factory* and the data dir as a plain
dependency so the unit suite can override them with in-memory fakes
(``app.dependency_overrides``) without any boto3/Kafka/Neo4j/PG infra — the
same isolation invariant the CLI keeps via ``_build_reset_clients``.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from src.config import get_settings
from src.pipeline.reset import PipelineResetter


def get_data_dir() -> Path:
    """Directory under which audit entries are written (``<data>/audit/``).

    Matches the CLI's ``_cmd_reset`` derivation so API-driven and CLI-driven
    resets land their audit log in the same place.
    """
    return Path(get_settings().data_raw_dir).parent


def get_resetter_factory() -> Callable[[], PipelineResetter]:
    """Return a *lazy* factory that builds the live resetter on demand.

    Returning a factory rather than a resetter is deliberate: FastAPI resolves
    a route's dependencies on EVERY request, including ``dry_run=True``. If we
    built the resetter here, every dry-run preview — the safe affordance
    ig#970's admin UI calls first — would eagerly open Kafka + Postgres
    connections (``_build_reset_clients`` reads ``KAFKA_BOOTSTRAP_SERVERS`` and
    eagerly connects). With a factory, the live boto3/Kafka/Neo4j/PG clients
    stay UNBUILT until a route actually invokes it in the non-dry-run branch,
    so a dry-run touches no infra.

    Reuses the CLI's ``_build_reset_clients`` so adapter construction lives in
    one place. Tests override this dependency with a factory returning a fake.
    """

    def _build() -> PipelineResetter:
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

    return _build
