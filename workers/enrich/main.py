"""enrich-worker entry point. Run as ``python -m workers.enrich.main``."""

from __future__ import annotations

import os

from workers.enrich.processor import EnrichProcessor
from workers.lib.log import configure_logging, get_logger
from workers.lib.object_store import ObjectStore
from workers.lib.runner import WorkerRunner, WorkerSettings

_logger = get_logger("workers.enrich")


def build_runner() -> WorkerRunner:
    from kafka import KafkaConsumer, KafkaProducer  # type: ignore[import-untyped]

    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    bucket = os.environ["PIPELINE_BUCKET"]
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")

    settings = WorkerSettings(
        worker_name="enrich-worker",
        consume_topic="pipeline.dedup.done",
        produce_topic="pipeline.enrich.done",
        consumer_group="enrich-worker",
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
        process=EnrichProcessor(store),
    )


def main() -> None:
    configure_logging()
    _logger.info("enrich_worker_starting")
    build_runner().run_forever()


if __name__ == "__main__":
    main()
