"""ingest-worker entry point. Run as ``python -m workers.ingest.main``."""

from __future__ import annotations

import os
import signal
from types import FrameType
from typing import TYPE_CHECKING

from workers.ingest.processor import IngestProcessor
from workers.lib.log import configure_logging, get_logger
from workers.lib.object_store import ObjectStore
from workers.lib.runner import WorkerRunner, WorkerSettings
from workers.lib.topics import PIPELINE_NORMALIZE_DONE

if TYPE_CHECKING:
    from neo4j import Driver

_logger = get_logger("workers.ingest")


def build_runner() -> tuple[WorkerRunner, Driver]:
    from kafka import KafkaConsumer, KafkaProducer  # type: ignore[import-untyped]
    from neo4j import GraphDatabase

    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    bucket = os.environ["PIPELINE_BUCKET"]
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    neo4j_uri = os.environ["NEO4J_URI"]
    neo4j_user = os.environ["NEO4J_USER"]
    neo4j_password = os.environ["NEO4J_PASSWORD"]

    settings = WorkerSettings(
        worker_name="ingest-worker",
        consume_topic=PIPELINE_NORMALIZE_DONE,
        produce_topic=None,  # terminal stage
        consumer_group="ingest-worker",
    )
    consumer = KafkaConsumer(
        settings.consume_topic,
        bootstrap_servers=bootstrap,
        group_id=settings.consumer_group,
        enable_auto_commit=False,
    )
    producer = KafkaProducer(bootstrap_servers=bootstrap)
    store = ObjectStore(bucket=bucket, endpoint_url=endpoint_url)
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    # Fail fast at startup if Neo4j is unreachable or the credentials
    # are wrong — otherwise every incoming message would DLQ.
    driver.verify_connectivity()

    runner = WorkerRunner(
        settings=settings,
        consumer=consumer,
        producer=producer,
        process=IngestProcessor(store, neo4j_driver=driver),
    )
    return runner, driver


def _install_shutdown_handler(driver: Driver, consumer: object) -> None:
    """Close the Neo4j driver and Kafka consumer on SIGTERM/SIGINT.

    The runner's ``run_forever`` loop exits naturally once the consumer
    is closed (iterator stops yielding), so this is the minimally
    invasive way to get a graceful shutdown without touching
    ``WorkerRunner``.
    """

    def _handle(signum: int, _frame: FrameType | None) -> None:
        _logger.info("ingest_shutdown_received", signal=signum)
        try:
            close = getattr(consumer, "close", None)
            if callable(close):
                close()
        except Exception as exc:  # noqa: BLE001 — best-effort shutdown
            _logger.warning("ingest_consumer_close_failed", error=str(exc))
        try:
            driver.close()
        except Exception as exc:  # noqa: BLE001 — best-effort shutdown
            _logger.warning("ingest_driver_close_failed", error=str(exc))

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)


def main() -> None:
    configure_logging()
    _logger.info("ingest_worker_starting")
    runner, driver = build_runner()
    _install_shutdown_handler(driver, runner.consumer)
    try:
        runner.run_forever()
    finally:
        driver.close()


if __name__ == "__main__":
    main()
