"""HTTP-layer tests for the admin reset endpoints (issue #70).

Covers the three contract bars the PR must hold:

1. **Auth gate** — admin-only is enforced (missing header -> 401,
   non-admin -> 403, admin -> through). The real ``require_admin`` runs;
   only ``verify_user_service_token`` is stubbed so no JWKS network call.
2. **Confirmation-required** — the destructive full reset rejects a request
   that lacks the ``OBLITERATE`` token *before* touching the reset surface.
3. **Calls the reset surface** — each endpoint drives the real
   ``PipelineResetter`` with the correct scope, and dry-run still writes an
   audit entry without invoking the live resetter.

No boto3/Kafka/Neo4j/PG: the resetter and data dir are injected via
``app.dependency_overrides``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

import src.api.auth as auth_mod
from src.api.app import create_app
from src.api.auth import require_admin
from src.api.deps import get_data_dir, get_resetter_factory
from src.pipeline.audit import AuditEntry, create_audit_entry
from src.pipeline.reset import ResetReport, ResetScope


class _FakeResetter:
    """Records the scope/confirmation it was called with; writes no infra."""

    def __init__(self) -> None:
        self.calls: list[tuple[ResetScope, str]] = []

    def reset(
        self, scope: ResetScope, *, confirmation_method: str = "not-required"
    ) -> tuple[ResetReport, AuditEntry, Path]:
        self.calls.append((scope, confirmation_method))
        report = ResetReport(
            level=scope.level,
            confirmation_method=confirmation_method,
            scope_stage=scope.stage,
            scope_source=scope.source,
        )
        entry = create_audit_entry(stage=f"reset-{scope.level}", duration_seconds=0.0)
        return report, entry, Path("/tmp/fake-audit.json")


class _FactorySpy:
    """Stands in for ``get_resetter_factory``'s return value.

    Counts how many times it is *invoked* (i.e. how many times a route asked
    to build the live resetter). A dry-run must never invoke it — that is the
    dry-run-inert property — so ``build_count`` lets tests assert no live
    client construction happens on a dry run.
    """

    def __init__(self, resetter: _FakeResetter) -> None:
        self.resetter = resetter
        self.build_count = 0

    def __call__(self) -> _FakeResetter:
        self.build_count += 1
        return self.resetter


@pytest.fixture
def fake_resetter() -> _FakeResetter:
    return _FakeResetter()


@pytest.fixture
def factory_spy(fake_resetter: _FakeResetter) -> _FactorySpy:
    return _FactorySpy(fake_resetter)


@pytest.fixture
def client(tmp_path: Path, factory_spy: _FactorySpy) -> TestClient:
    """App with a fake resetter factory + tmp data dir, admin guard satisfied."""
    app = create_app()
    app.dependency_overrides[get_resetter_factory] = lambda: factory_spy
    app.dependency_overrides[get_data_dir] = lambda: tmp_path
    # Authenticated-admin caller for the non-auth-focused tests.
    app.dependency_overrides[require_admin] = lambda: "admin-user-id"
    return TestClient(app)


# ---------------------------------------------------------------------------
# 1. Auth gate — exercise the REAL require_admin
# ---------------------------------------------------------------------------


def _client_with_real_guard(tmp_path: Path, factory_spy: _FactorySpy) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_resetter_factory] = lambda: factory_spy
    app.dependency_overrides[get_data_dir] = lambda: tmp_path
    return TestClient(app, raise_server_exceptions=True)


def test_missing_auth_header_rejected(
    tmp_path: Path, fake_resetter: _FakeResetter, factory_spy: _FactorySpy
) -> None:
    c = _client_with_real_guard(tmp_path, factory_spy)
    resp = c.post("/admin/reset/stage", json={"stage": "raw"})
    assert resp.status_code == 401
    assert fake_resetter.calls == []


def test_non_admin_token_forbidden(
    tmp_path: Path,
    fake_resetter: _FakeResetter,
    factory_spy: _FactorySpy,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        auth_mod,
        "verify_user_service_token",
        lambda _token: {"type": "access", "sub": "u1", "roles": ["viewer"]},
    )
    c = _client_with_real_guard(tmp_path, factory_spy)
    resp = c.post(
        "/admin/reset/stage",
        json={"stage": "raw"},
        headers={"Authorization": "Bearer faketoken"},
    )
    assert resp.status_code == 403
    assert fake_resetter.calls == []


def test_admin_token_allowed(
    tmp_path: Path,
    fake_resetter: _FakeResetter,
    factory_spy: _FactorySpy,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        auth_mod,
        "verify_user_service_token",
        lambda _token: {"type": "access", "sub": "admin1", "roles": ["admin"]},
    )
    c = _client_with_real_guard(tmp_path, factory_spy)
    resp = c.post(
        "/admin/reset/stage",
        json={"stage": "raw"},
        headers={"Authorization": "Bearer admintoken"},
    )
    assert resp.status_code == 200
    assert len(fake_resetter.calls) == 1


def test_non_access_token_type_rejected(
    tmp_path: Path,
    fake_resetter: _FakeResetter,
    factory_spy: _FactorySpy,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        auth_mod,
        "verify_user_service_token",
        lambda _token: {"type": "refresh", "sub": "admin1", "roles": ["admin"]},
    )
    c = _client_with_real_guard(tmp_path, factory_spy)
    resp = c.post(
        "/admin/reset/stage",
        json={"stage": "raw"},
        headers={"Authorization": "Bearer refreshtoken"},
    )
    assert resp.status_code == 401
    assert fake_resetter.calls == []


def test_jwks_unreachable_returns_503(
    tmp_path: Path,
    fake_resetter: _FakeResetter,
    factory_spy: _FactorySpy,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boom(_token: str) -> dict[str, object]:
        raise httpx.ConnectError("user-service down")

    monkeypatch.setattr(auth_mod, "verify_user_service_token", _boom)
    c = _client_with_real_guard(tmp_path, factory_spy)
    resp = c.post(
        "/admin/reset/stage",
        json={"stage": "raw"},
        headers={"Authorization": "Bearer anytoken"},
    )
    assert resp.status_code == 503
    assert fake_resetter.calls == []


# ---------------------------------------------------------------------------
# 2. Confirmation-required for the destructive full reset
# ---------------------------------------------------------------------------


def test_full_reset_without_confirmation_rejected(
    client: TestClient, fake_resetter: _FakeResetter
) -> None:
    # Field omitted -> 422 (schema requires it), and nothing executed.
    resp = client.post("/admin/reset/full", json={})
    assert resp.status_code == 422
    assert fake_resetter.calls == []


def test_full_reset_wrong_confirmation_rejected(
    client: TestClient, fake_resetter: _FakeResetter
) -> None:
    resp = client.post("/admin/reset/full", json={"confirmation": "yes please"})
    assert resp.status_code == 400
    assert fake_resetter.calls == []


def test_full_reset_with_obliterate_executes(
    client: TestClient, fake_resetter: _FakeResetter
) -> None:
    resp = client.post("/admin/reset/full", json={"confirmation": "OBLITERATE"})
    assert resp.status_code == 200
    assert len(fake_resetter.calls) == 1
    scope, confirmation_method = fake_resetter.calls[0]
    assert scope.level == "full"
    # Issue #74: the OBLITERATE token over HTTP records the distinct "api"
    # method (not "interactive") so SIEM can attribute it to the admin endpoint.
    # The endpoint fixes this value itself — it is not read from the request body.
    assert confirmation_method == "api"
    body: dict[str, Any] = resp.json()
    assert body["level"] == "full"
    assert body["confirmation_method"] == "api"


# ---------------------------------------------------------------------------
# 3. Endpoints drive the real reset surface with the right scope
# ---------------------------------------------------------------------------


def test_stage_reset_calls_resetter_with_stage_scope(
    client: TestClient, fake_resetter: _FakeResetter
) -> None:
    resp = client.post("/admin/reset/stage", json={"stage": "dedup"})
    assert resp.status_code == 200
    scope, method = fake_resetter.calls[0]
    assert scope.level == "stage"
    assert scope.stage == "dedup"
    assert method == "not-required"


def test_source_reset_calls_resetter_with_source_scope(
    client: TestClient, fake_resetter: _FakeResetter
) -> None:
    resp = client.post("/admin/reset/source", json={"source": "sunnah-api"})
    assert resp.status_code == 200
    scope, _method = fake_resetter.calls[0]
    assert scope.level == "source"
    assert scope.source == "sunnah-api"


def test_unknown_stage_rejected_before_reset(
    client: TestClient, fake_resetter: _FakeResetter
) -> None:
    resp = client.post("/admin/reset/stage", json={"stage": "bogus"})
    assert resp.status_code == 422
    assert fake_resetter.calls == []


def test_invalid_source_rejected_before_reset(
    client: TestClient, fake_resetter: _FakeResetter
) -> None:
    resp = client.post("/admin/reset/source", json={"source": "bad/source"})
    assert resp.status_code == 422
    assert fake_resetter.calls == []


def test_dry_run_writes_audit_without_invoking_resetter(
    client: TestClient, fake_resetter: _FakeResetter, factory_spy: _FactorySpy, tmp_path: Path
) -> None:
    resp = client.post("/admin/reset/full", json={"confirmation": "OBLITERATE", "dry_run": True})
    assert resp.status_code == 200
    # Dry-run goes through write_dry_run_audit, NOT the live resetter.
    assert fake_resetter.calls == []
    body = resp.json()
    assert body["dry_run"] is True
    # A real audit entry was written under the injected data dir.
    audit_files = list((tmp_path / "audit").glob("*.json"))
    assert len(audit_files) == 1
    assert Path(body["audit_entry_path"]) == audit_files[0]


@pytest.mark.parametrize(
    ("path", "payload"),
    [
        ("/admin/reset/stage", {"stage": "raw", "dry_run": True}),
        ("/admin/reset/source", {"source": "sunnah-api", "dry_run": True}),
        ("/admin/reset/full", {"confirmation": "OBLITERATE", "dry_run": True}),
    ],
)
def test_dry_run_never_builds_the_resetter(
    client: TestClient,
    factory_spy: _FactorySpy,
    path: str,
    payload: dict[str, object],
) -> None:
    """Regression (Petra, #73): a dry-run must NOT construct the live resetter.

    The resetter factory builds boto3/Kafka/Neo4j/PG clients (eager Kafka +
    PG connects). If a route resolved it eagerly, a dry-run preview — the safe
    affordance ig#970's admin UI calls first — would open those connections.
    ``build_count == 0`` proves the factory is never invoked on a dry run, so
    no live client is constructed.
    """
    resp = client.post(path, json=payload)
    assert resp.status_code == 200
    assert factory_spy.build_count == 0


def test_real_reset_builds_the_resetter_once(client: TestClient, factory_spy: _FactorySpy) -> None:
    """Counterpart: a non-dry-run reset DOES build the resetter (exactly once)."""
    resp = client.post("/admin/reset/stage", json={"stage": "raw"})
    assert resp.status_code == 200
    assert factory_spy.build_count == 1
