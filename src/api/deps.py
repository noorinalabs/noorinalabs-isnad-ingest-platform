"""FastAPI dependencies for the reset / metrics / reprocess HTTP surface.

The resetter and reprocessor are provided as *lazy factories* and the data
dir / metrics readers as plain dependencies so the unit suite can override
them with in-memory fakes (``app.dependency_overrides``) without any
boto3/Kafka/Neo4j/PG infra — the same isolation invariant the CLI keeps via
``_build_reset_clients``.

The read-only metrics dependencies (``get_kafka_lag_reader`` /
``get_object_size_store``) return objects whose live clients are built
*lazily on first use*, so resolving the dependency opens no connection;
the connection happens only when a report computation actually reads
offsets / lists objects. The destructive reprocess dependency keeps the
reset surface's factory shape so a dry-run never constructs a live client.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.config import get_settings
from src.pipeline.metrics import KafkaOffsetReader, ObjectSizeStore
from src.pipeline.reprocess import PipelineReprocessor
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


# ---------------------------------------------------------------------------
# Metrics dependencies (read-only; lazy live clients)
# ---------------------------------------------------------------------------


class _BotoSizeStore:
    """Minimal ``ObjectSizeStore`` over a lazily built boto3 S3/B2 client.

    Re-declared here (rather than importing ``workers.lib.object_store``) so
    ``src`` stays independent of ``workers``, matching the inline ``_Store``
    the reset CLI builder uses. Constructing it opens no connection — the
    boto3 client is built on first ``client`` access.
    """

    def __init__(self, bucket: str, endpoint_url: str | None) -> None:
        self.bucket = bucket
        self._endpoint_url = endpoint_url
        self._client: Any | None = None

    @property
    def client(self) -> Any:
        if self._client is None:
            import boto3  # type: ignore[import-untyped]

            kwargs: dict[str, Any] = {}
            if self._endpoint_url:
                kwargs["endpoint_url"] = self._endpoint_url
            self._client = boto3.client("s3", **kwargs)
        return self._client


def get_object_size_store() -> ObjectSizeStore:
    """Return an object store for the storage-size metric (lazy boto3 client).

    Reads the bucket / endpoint from the same env vars the workers and reset
    CLI use (``PIPELINE_BUCKET`` / ``S3_ENDPOINT_URL``). Tests override this
    with an in-memory fake.
    """
    return _BotoSizeStore(
        bucket=os.environ.get("PIPELINE_BUCKET", "noorinalabs-pipeline"),
        endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
    )


def get_kafka_lag_reader() -> KafkaOffsetReader:
    """Return a Kafka offset reader for the consumer-lag metric.

    The adapter builds its admin + consumer clients lazily, so resolving this
    dependency opens no broker connection. Tests override it with a fake.
    """
    from src.pipeline.metrics_adapters import KafkaLagAdapter

    return KafkaLagAdapter(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"])


# ---------------------------------------------------------------------------
# Reprocess dependency (destructive-ish; lazy factory like the resetter)
# ---------------------------------------------------------------------------


def get_reprocessor_factory() -> Callable[[], PipelineReprocessor]:
    """Return a *lazy* factory that builds the live reprocessor on demand.

    Mirrors ``get_resetter_factory``: a reprocess dry-run must not open Kafka
    or Postgres connections, so the live clients stay unbuilt until a route
    invokes the factory in the non-dry-run branch.
    """

    def _build() -> PipelineReprocessor:
        import psycopg
        from kafka.admin import KafkaAdminClient  # type: ignore[import-untyped]

        from src.pipeline.reprocess_adapters import PgCheckpointResetter
        from src.pipeline.reset_adapters import KafkaPythonAdmin

        settings = get_settings()
        bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
        return PipelineReprocessor(
            kafka=KafkaPythonAdmin(KafkaAdminClient(bootstrap_servers=bootstrap)),
            checkpoints=PgCheckpointResetter(psycopg.connect(settings.postgres.dsn)),
            data_dir=get_data_dir(),
        )

    return _build
