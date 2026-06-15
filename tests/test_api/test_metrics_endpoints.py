"""HTTP-layer tests for the admin metrics endpoints (issue #76).

Covers the contract bars:

1. **Auth gate** — both metric routes are admin-only (missing header -> 401;
   admin -> through), exercising the REAL ``require_admin``.
2. **Lag shape** — ``GET /admin/metrics/lag`` returns per-stage + total lag
   computed from the injected offset reader.
3. **Storage shape** — ``GET /admin/metrics/storage`` returns per-prefix +
   rollup sizes from the injected object store.

The Kafka offset reader and object store are injected via
``app.dependency_overrides`` — no broker, no S3/B2.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

import src.api.auth as auth_mod
from src.api.app import create_app
from src.api.auth import require_admin
from src.api.deps import get_kafka_lag_reader, get_object_size_store


class _FakeOffsetReader:
    """Every topic: one partition, head 100; no committed offset -> lag 100."""

    def end_offsets(self, topic: str) -> dict[int, int]:
        return {0: 100}

    def committed_offsets(self, group_id: str, topic: str) -> dict[int, int]:
        return {0: 90}


class _FakeS3Client:
    def list_objects_v2(
        self, *, Bucket: str, Prefix: str, ContinuationToken: str | None = None
    ) -> dict[str, Any]:
        if Prefix == "raw/":
            return {"Contents": [{"Key": "raw/a.parquet", "Size": 5}], "IsTruncated": False}
        return {"Contents": [], "IsTruncated": False}


class _FakeStore:
    bucket = "noorinalabs-pipeline"

    @property
    def client(self) -> _FakeS3Client:
        return _FakeS3Client()


@pytest.fixture
def client() -> TestClient:
    app = create_app()
    app.dependency_overrides[get_kafka_lag_reader] = lambda: _FakeOffsetReader()
    app.dependency_overrides[get_object_size_store] = lambda: _FakeStore()
    app.dependency_overrides[require_admin] = lambda: "admin-user-id"
    return TestClient(app)


# --- Auth gate (real guard) --------------------------------------------------


def _client_real_guard() -> TestClient:
    app = create_app()
    app.dependency_overrides[get_kafka_lag_reader] = lambda: _FakeOffsetReader()
    app.dependency_overrides[get_object_size_store] = lambda: _FakeStore()
    return TestClient(app, raise_server_exceptions=True)


def test_lag_requires_auth() -> None:
    assert _client_real_guard().get("/admin/metrics/lag").status_code == 401


def test_storage_requires_auth() -> None:
    assert _client_real_guard().get("/admin/metrics/storage").status_code == 401


def test_admin_token_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        auth_mod,
        "verify_user_service_token",
        lambda _t: {"type": "access", "sub": "a1", "roles": ["admin"]},
    )
    resp = _client_real_guard().get(
        "/admin/metrics/lag", headers={"Authorization": "Bearer admintoken"}
    )
    assert resp.status_code == 200


# --- Response shapes ---------------------------------------------------------


def test_lag_response_shape(client: TestClient) -> None:
    body = client.get("/admin/metrics/lag").json()
    assert [s["stage"] for s in body["stages"]] == [
        "dedup",
        "enrich",
        "normalize",
        "graph-load",
    ]
    dedup = body["stages"][0]
    assert dedup["consumer_group"] == "dedup-worker"
    assert dedup["topic"] == "pipeline.raw.landed"
    assert dedup["total_lag"] == 10
    assert dedup["partitions"][0] == {
        "partition": 0,
        "end_offset": 100,
        "committed_offset": 90,
        "lag": 10,
    }
    assert body["total_lag"] == 40  # 4 stages * 10


def test_storage_response_shape(client: TestClient) -> None:
    body = client.get("/admin/metrics/storage").json()
    by_prefix = {p["prefix"]: p for p in body["prefixes"]}
    assert set(by_prefix) == {"raw/", "dedup/", "enriched/", "normalized/", "staged/"}
    assert by_prefix["raw/"] == {"prefix": "raw/", "object_count": 1, "total_bytes": 5}
    assert by_prefix["dedup/"]["object_count"] == 0
    assert body["total_object_count"] == 1
    assert body["total_bytes"] == 5
