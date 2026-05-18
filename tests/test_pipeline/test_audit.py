"""Tests for the pipeline audit trail system."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.pipeline.audit import (
    AuditEntry,
    create_audit_entry,
    list_recent_entries,
    write_audit_entry,
)


@pytest.fixture()
def data_dir(tmp_path: Path) -> Path:
    return tmp_path


class TestAuditEntry:
    def test_create_audit_entry(self) -> None:
        entry = create_audit_entry(
            "load",
            duration_seconds=12.5,
            files_changed=[{"file": "staging/a.parquet", "md5_after": "abc"}],
            rows_affected=100,
            summary={"total_nodes": 100},
        )
        assert entry.stage == "load"
        assert entry.duration_seconds == 12.5
        assert entry.rows_affected == 100
        assert len(entry.files_changed) == 1
        assert entry.summary["total_nodes"] == 100
        assert entry.operator  # should be populated
        assert entry.timestamp  # should be populated


class TestWriteAndRead:
    def test_write_creates_file(self, data_dir: Path) -> None:
        entry = create_audit_entry("sync", duration_seconds=5.0)
        path = write_audit_entry(data_dir, entry)
        assert path.exists()
        assert path.suffix == ".json"
        assert "sync" in path.name

    def test_write_creates_audit_dir(self, data_dir: Path) -> None:
        entry = create_audit_entry("load", duration_seconds=1.0)
        write_audit_entry(data_dir, entry)
        assert (data_dir / "audit").is_dir()

    def test_list_recent_entries(self, data_dir: Path) -> None:
        for stage in ("sync", "load", "enrich"):
            entry = create_audit_entry(stage, duration_seconds=1.0)
            write_audit_entry(data_dir, entry)

        entries = list_recent_entries(data_dir, last_n=10)
        assert len(entries) == 3
        # All entries should be AuditEntry instances
        for e in entries:
            assert isinstance(e, AuditEntry)

    def test_list_recent_entries_limit(self, data_dir: Path) -> None:
        for i in range(5):
            entry = create_audit_entry(f"stage{i}", duration_seconds=float(i))
            write_audit_entry(data_dir, entry)

        entries = list_recent_entries(data_dir, last_n=2)
        assert len(entries) == 2

    def test_list_recent_entries_empty_dir(self, data_dir: Path) -> None:
        entries = list_recent_entries(data_dir)
        assert entries == []

    def test_round_trip_preserves_data(self, data_dir: Path) -> None:
        entry = create_audit_entry(
            "load",
            duration_seconds=42.0,
            files_changed=[{"file": "staging/x.parquet", "md5_before": "aaa", "md5_after": "bbb"}],
            rows_affected=500,
            summary={"incremental": True, "files_skipped": 3},
        )
        write_audit_entry(data_dir, entry)

        loaded = list_recent_entries(data_dir, last_n=1)
        assert len(loaded) == 1
        e = loaded[0]
        assert e.stage == "load"
        assert e.duration_seconds == 42.0
        assert e.rows_affected == 500
        assert len(e.files_changed) == 1
        assert e.files_changed[0]["md5_before"] == "aaa"
        assert e.summary["incremental"] is True


class TestCallerHints:
    """Issue #16 bonus: SIEM needs to attribute audit entries to a human
    even when ``getpass.getuser()`` returns ``root`` (sudo), a service
    account (k8s), or a CI bot account (GitHub Actions). Env-derived
    hints are collected into ``summary["caller_hints"]``."""

    _ALL_HINT_VARS = ("SUDO_USER", "GITHUB_ACTOR", "KUBERNETES_SERVICE_ACCOUNT")

    def _clear_hint_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for var in self._ALL_HINT_VARS:
            monkeypatch.delenv(var, raising=False)

    def test_no_hints_when_env_vars_absent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._clear_hint_env(monkeypatch)

        entry = create_audit_entry("sync", duration_seconds=1.0)

        assert "caller_hints" not in entry.summary

    def test_sudo_user_recorded_when_present(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._clear_hint_env(monkeypatch)
        monkeypatch.setenv("SUDO_USER", "alice")

        entry = create_audit_entry("sync", duration_seconds=1.0)

        assert entry.summary["caller_hints"] == {"SUDO_USER": "alice"}

    def test_github_actor_and_k8s_sa_both_recorded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._clear_hint_env(monkeypatch)
        monkeypatch.setenv("GITHUB_ACTOR", "bob-bot")
        monkeypatch.setenv("KUBERNETES_SERVICE_ACCOUNT", "audit-runner")

        entry = create_audit_entry("reset-full", duration_seconds=1.0)

        assert entry.summary["caller_hints"] == {
            "GITHUB_ACTOR": "bob-bot",
            "KUBERNETES_SERVICE_ACCOUNT": "audit-runner",
        }

    def test_empty_env_var_value_treated_as_absent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._clear_hint_env(monkeypatch)
        monkeypatch.setenv("SUDO_USER", "")

        entry = create_audit_entry("sync", duration_seconds=1.0)

        assert "caller_hints" not in entry.summary

    def test_caller_hints_merge_with_existing_summary(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._clear_hint_env(monkeypatch)
        monkeypatch.setenv("SUDO_USER", "alice")

        entry = create_audit_entry(
            "reset-full",
            duration_seconds=1.0,
            summary={"level": "full", "s3_objects_deleted": 0},
        )

        assert entry.summary["level"] == "full"
        assert entry.summary["s3_objects_deleted"] == 0
        assert entry.summary["caller_hints"] == {"SUDO_USER": "alice"}

    def test_caller_hints_does_not_mutate_input_summary(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._clear_hint_env(monkeypatch)
        monkeypatch.setenv("SUDO_USER", "alice")
        input_summary: dict[str, object] = {"level": "stage"}

        create_audit_entry("reset-stage", duration_seconds=1.0, summary=input_summary)

        assert "caller_hints" not in input_summary
