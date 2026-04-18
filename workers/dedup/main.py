"""dedup-worker entry point. Run as ``python -m workers.dedup.main``."""

from __future__ import annotations

import os

from workers.dedup.processor import DedupProcessor
from workers.lib.log import configure_logging, get_logger
from workers.lib.object_store import ObjectStore
from workers.lib.runner import WorkerRunner, WorkerSettings

_logger = get_logger("workers.dedup")


def build_runner() -> WorkerRunner:
    """Wire the dedup worker. Kept pure so tests can swap deps."""
    # Imported lazily so unit tests don't require kafka-python / boto3.
    from kafka import KafkaConsumer, KafkaProducer  # type: ignore[import-untyped]

    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    bucket = os.environ["PIPELINE_BUCKET"]
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")

    settings = WorkerSettings(
        worker_name="dedup-worker",
        consume_topic="pipeline.raw.new",
        produce_topic="pipeline.dedup.done",
        consumer_group="dedup-worker",
    )
    consumer = KafkaConsumer(
        settings.consume_topic,
        bootstrap_servers=bootstrap,
        group_id=settings.consumer_group,
        enable_auto_commit=False,
    )
    producer = KafkaProducer(bootstrap_servers=bootstrap)
    store = ObjectStore(bucket=bucket, endpoint_url=endpoint_url)

    return WorkerRunner(
        settings=settings,
        consumer=consumer,
        producer=producer,
        process=DedupProcessor(store),
    )


def main() -> None:
    configure_logging()
    _logger.info("dedup_worker_starting")
    build_runner().run_forever()


if __name__ == "__main__":
    main()
