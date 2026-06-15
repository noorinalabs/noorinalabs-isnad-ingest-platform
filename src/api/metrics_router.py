"""Admin-only HTTP endpoints exposing pipeline metrics (issue #76).

Two read-only GET endpoints the isnad-graph admin data panel polls:

- ``GET /admin/metrics/lag``     — per-stage Kafka consumer lag + total
- ``GET /admin/metrics/storage`` — per-B2-prefix object count/bytes + total

Both delegate to the pure report functions in
:mod:`src.pipeline.metrics`, computed over injected readers
(``get_kafka_lag_reader`` / ``get_object_size_store``). The admin guard is
applied at the router mount point in :mod:`src.api.app`
(``dependencies=[Depends(require_admin)]``), so both routes are admin-only.

Response shapes are the contract the admin panel consumes — see
``docs/pipeline-metrics-api.md``.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict

from src.api.deps import get_kafka_lag_reader, get_object_size_store
from src.pipeline.metrics import (
    KafkaOffsetReader,
    ObjectSizeStore,
    compute_lag_report,
    compute_storage_report,
)

router = APIRouter(prefix="/metrics", tags=["admin", "metrics"])


class PartitionLagModel(BaseModel):
    """Lag for one (topic, partition) under a stage's consumer group."""

    model_config = ConfigDict(frozen=True)

    partition: int
    end_offset: int
    committed_offset: int | None
    lag: int


class StageLagModel(BaseModel):
    """Lag for one pipeline stage's consumer group on its consume topic."""

    model_config = ConfigDict(frozen=True)

    stage: str
    consumer_group: str
    topic: str
    total_lag: int
    partitions: list[PartitionLagModel]


class LagResponse(BaseModel):
    """Consumer lag across every pipeline stage."""

    model_config = ConfigDict(frozen=True)

    stages: list[StageLagModel]
    total_lag: int


class PrefixSizeModel(BaseModel):
    """Object count + byte total under one B2 prefix."""

    model_config = ConfigDict(frozen=True)

    prefix: str
    object_count: int
    total_bytes: int


class StorageResponse(BaseModel):
    """Object-store size broken down by stage prefix, plus the rollup."""

    model_config = ConfigDict(frozen=True)

    prefixes: list[PrefixSizeModel]
    total_object_count: int
    total_bytes: int


@router.get("/lag", response_model=LagResponse)
def metrics_lag(
    reader: Annotated[KafkaOffsetReader, Depends(get_kafka_lag_reader)],
) -> LagResponse:
    """Per-stage Kafka consumer lag and the pipeline-wide total."""
    report = compute_lag_report(reader)
    return LagResponse(
        stages=[
            StageLagModel(
                stage=s.stage,
                consumer_group=s.consumer_group,
                topic=s.topic,
                total_lag=s.total_lag,
                partitions=[
                    PartitionLagModel(
                        partition=p.partition,
                        end_offset=p.end_offset,
                        committed_offset=p.committed_offset,
                        lag=p.lag,
                    )
                    for p in s.partitions
                ],
            )
            for s in report.stages
        ],
        total_lag=report.total_lag,
    )


@router.get("/storage", response_model=StorageResponse)
def metrics_storage(
    store: Annotated[ObjectSizeStore, Depends(get_object_size_store)],
) -> StorageResponse:
    """Per-B2-prefix object count + bytes and the bucket-wide rollup."""
    report = compute_storage_report(store)
    return StorageResponse(
        prefixes=[
            PrefixSizeModel(
                prefix=p.prefix,
                object_count=p.object_count,
                total_bytes=p.total_bytes,
            )
            for p in report.prefixes
        ],
        total_object_count=report.total_object_count,
        total_bytes=report.total_bytes,
    )
