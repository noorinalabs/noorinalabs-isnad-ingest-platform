"""ingest-worker entry point. Run as ``python -m workers.ingest.main``."""

from __future__ import annotations

import os

from workers.ingest.processor import IngestProcessor
from workers.lib.log import configure_logging, get_logger
from workers.lib.object_store import ObjectStore
from workers.lib.runner import WorkerRunner, WorkerSettings

_logger = get_logger("workers.ingest")


def build_runner() -> WorkerRunner:
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
        consume_topic="pipeline.norm.done",
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

    return WorkerRunner(
        settings=settings,
        consumer=consumer,
        producer=producer,
        process=IngestProcessor(store, neo4j_driver=driver),
    )


def main() -> None:
    configure_logging()
    _logger.info("ingest_worker_starting")
    build_runner().run_forever()


if __name__ == "__main__":
    main()
