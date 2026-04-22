"""normalize-worker entry point. Run as ``python -m workers.normalize.main``."""

from __future__ import annotations

import os

from workers.lib.log import configure_logging, get_logger
from workers.lib.object_store import ObjectStore
from workers.lib.runner import WorkerRunner, WorkerSettings
from workers.lib.topics import PIPELINE_ENRICH_DONE, PIPELINE_NORMALIZE_DONE
from workers.normalize.processor import NormalizeProcessor

_logger = get_logger("workers.normalize")


def build_runner() -> WorkerRunner:
    from kafka import KafkaConsumer, KafkaProducer  # type: ignore[import-untyped]

    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    bucket = os.environ["PIPELINE_BUCKET"]
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")

    settings = WorkerSettings(
        worker_name="normalize-worker",
        consume_topic=PIPELINE_ENRICH_DONE,
        produce_topic=PIPELINE_NORMALIZE_DONE,
        consumer_group="normalize-worker",
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
        process=NormalizeProcessor(store),
    )


def main() -> None:
    configure_logging()
    _logger.info("normalize_worker_starting")
    build_runner().run_forever()


if __name__ == "__main__":
    main()
