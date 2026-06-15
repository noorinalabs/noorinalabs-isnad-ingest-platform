"""HTTP-layer tests for the admin reprocess endpoint (issue #76).

Covers the contract bars:

1. **Auth gate** — admin-only (missing header -> 401; admin -> through),
   exercising the REAL ``require_admin``.
2. **Unknown stage rejected** — a bad stage name is a 422 *before* any client
   is built.
3. **Dry-run is inert** — a dry-run writes an audit entry and NEVER invokes
   the reprocessor factory (no live Kafka/PG client construction).
4. **Live path** — a non-dry-run reprocess builds the reprocessor once and
   drives it with the requested stage.

The reprocessor factory + data dir are injected via
``app.dependency_overrides`` — no Kafka, no Postgres.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

import src.api.auth as auth_mod
from src.api.app import create_app
from src.api.auth import require_admin
from src.api.deps import get_data_dir, get_reprocessor_factory
from src.pipeline.audit import AuditEntry, create_audit_entry
from src.pipeline.reprocess import ReprocessReport


class _FakeReprocessor:
    """Records the stage it was asked to reprocess; writes no infra."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def reprocess(self, stage_name: str) -> tuple[ReprocessReport, AuditEntry, Path]:
        self.calls.append(stage_name)
        report = ReprocessReport(
            from_stage=stage_name,
            rewound_group=f"{stage_name}-worker",
            rewound_topic="pipeline.test.topic",
            affected_stages=[stage_name],
            checkpoints_cleared={stage_name: 0},
        )
        entry = create_audit_entry(stage=f"reprocess-{stage_name}", duration_seconds=0.0)
        return report, entry, Path("/tmp/fake-audit.json")


class _FactorySpy:
    def __init__(self, reprocessor: _FakeReprocessor) -> None:
        self.reprocessor = reprocessor
        self.build_count = 0

    def __call__(self) -> _FakeReprocessor:
        self.build_count += 1
        return self.reprocessor


@pytest.fixture
def fake_reprocessor() -> _FakeReprocessor:
    return _FakeReprocessor()


@pytest.fixture
def factory_spy(fake_reprocessor: _FakeReprocessor) -> _FactorySpy:
    return _FactorySpy(fake_reprocessor)


@pytest.fixture
def client(tmp_path: Path, factory_spy: _FactorySpy) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_reprocessor_factory] = lambda: factory_spy
    app.dependency_overrides[get_data_dir] = lambda: tmp_path
    app.dependency_overrides[require_admin] = lambda: "admin-user-id"
    return TestClient(app)


# --- Auth gate ---------------------------------------------------------------


def _client_real_guard(tmp_path: Path, factory_spy: _FactorySpy) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_reprocessor_factory] = lambda: factory_spy
    app.dependency_overrides[get_data_dir] = lambda: tmp_path
    return TestClient(app, raise_server_exceptions=True)


def test_missing_auth_rejected(
    tmp_path: Path, fake_reprocessor: _FakeReprocessor, factory_spy: _FactorySpy
) -> None:
    c = _client_real_guard(tmp_path, factory_spy)
    resp = c.post("/admin/reprocess", json={"stage": "enrich"})
    assert resp.status_code == 401
    assert fake_reprocessor.calls == []


def test_admin_token_allowed(
    tmp_path: Path,
    factory_spy: _FactorySpy,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        auth_mod,
        "verify_user_service_token",
        lambda _t: {"type": "access", "sub": "a1", "roles": ["admin"]},
    )
    c = _client_real_guard(tmp_path, factory_spy)
    resp = c.post(
        "/admin/reprocess",
        json={"stage": "enrich"},
        headers={"Authorization": "Bearer admintoken"},
    )
    assert resp.status_code == 200


# --- Behaviour ---------------------------------------------------------------


def test_unknown_stage_rejected_before_build(
    client: TestClient, fake_reprocessor: _FakeReprocessor, factory_spy: _FactorySpy
) -> None:
    resp = client.post("/admin/reprocess", json={"stage": "bogus"})
    assert resp.status_code == 422
    assert fake_reprocessor.calls == []
    assert factory_spy.build_count == 0


def test_live_reprocess_builds_factory_once_and_calls_stage(
    client: TestClient, fake_reprocessor: _FakeReprocessor, factory_spy: _FactorySpy
) -> None:
    resp = client.post("/admin/reprocess", json={"stage": "normalize"})
    assert resp.status_code == 200
    assert factory_spy.build_count == 1
    assert fake_reprocessor.calls == ["normalize"]
    body: dict[str, Any] = resp.json()
    assert body["from_stage"] == "normalize"
    assert body["dry_run"] is False
    assert body["summary"]["from_stage"] == "normalize"


def test_dry_run_is_inert_and_writes_audit(
    client: TestClient, fake_reprocessor: _FakeReprocessor, factory_spy: _FactorySpy, tmp_path: Path
) -> None:
    resp = client.post("/admin/reprocess", json={"stage": "dedup", "dry_run": True})
    assert resp.status_code == 200
    # Dry-run never builds the live reprocessor.
    assert factory_spy.build_count == 0
    assert fake_reprocessor.calls == []
    body = resp.json()
    assert body["dry_run"] is True
    assert body["from_stage"] == "dedup"
    audit_files = list((tmp_path / "audit").glob("*.json"))
    assert len(audit_files) == 1
    assert Path(body["audit_entry_path"]) == audit_files[0]
