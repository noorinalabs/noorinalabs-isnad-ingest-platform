"""Tests for pre_commit_ci_sync — the pre-commit <-> CI drift gate (#327).

Verifies:
  1. Canonical kind extraction from both pre-commit configs and CI workflows.
  2. The drift direction that gates: CI-enforced-but-not-local is harmful;
     local-but-not-CI is stricter-local (informational, never a gate fail).
  3. ruff-format vs ruff-lint are not conflated.
  4. This repo's (noorinalabs-isnad-ingest-platform) config mirrors ALL of its
     CI-enforced kinds (no harmful drift) — the gate running against the very
     repo that ships it. The gate is wired UNSCOPED here: it scans every
     workflow in .github/workflows/ (ci.yml AND docs.yml), so the pre-commit
     config must also mirror docs.yml's actionlint, not just ci.yml's
     ruff/mypy/pytest.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

# Helper lives at .claude/lib/pre_commit_ci_sync.py; test is at
# .claude/lib/tests/test_*.py. parent.parent reaches the lib root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pre_commit_ci_sync import (
    check_repo,
    compute_drift,
    kinds_from_ci,
    kinds_from_precommit,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]


class PrecommitKindExtraction(unittest.TestCase):
    def test_ruff_format_and_lint_both_detected(self) -> None:
        cfg = """
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    hooks:
      - id: ruff-format
      - id: ruff
"""
        kinds = kinds_from_precommit(cfg)
        self.assertIn("ruff-format", kinds)
        self.assertIn("ruff-lint", kinds)

    def test_mypy_and_pytest_local_hooks(self) -> None:
        cfg = """
repos:
  - repo: local
    hooks:
      - id: mypy
        entry: python3 -m mypy
      - id: pytest-unit
        entry: pytest
"""
        kinds = kinds_from_precommit(cfg)
        self.assertIn("mypy", kinds)
        self.assertIn("pytest", kinds)

    def test_actionlint_hook_detected(self) -> None:
        cfg = """
repos:
  - repo: https://github.com/rhysd/actionlint
    hooks:
      - id: actionlint
"""
        self.assertIn("actionlint", kinds_from_precommit(cfg))

    def test_comments_ignored(self) -> None:
        cfg = "# id: mypy is just a comment\nrepos: []\n"
        self.assertNotIn("mypy", kinds_from_precommit(cfg))


class CiKindExtraction(unittest.TestCase):
    def test_run_steps_detected(self) -> None:
        wf = """
jobs:
  lint:
    steps:
      - run: ruff check .
      - run: ruff format --check .
      - run: mypy src/
      - run: pytest -q
"""
        kinds = kinds_from_ci(wf)
        self.assertEqual(
            kinds & {"ruff-lint", "ruff-format", "mypy", "pytest"},
            {"ruff-lint", "ruff-format", "mypy", "pytest"},
        )

    def test_actionlint_run_detected(self) -> None:
        wf = """
jobs:
  lint:
    steps:
      - run: ./actionlint -color
"""
        self.assertIn("actionlint", kinds_from_ci(wf))

    def test_ruff_format_line_not_counted_as_lint(self) -> None:
        # A format-only line must NOT register ruff-lint.
        kinds = kinds_from_ci("      - run: ruff format --check .\n")
        self.assertIn("ruff-format", kinds)
        self.assertNotIn("ruff-lint", kinds)


class DriftDirection(unittest.TestCase):
    def test_ci_enforced_not_local_is_harmful(self) -> None:
        harmful, stricter = compute_drift(
            precommit_kinds={"ruff-lint"},
            ci_kinds={"ruff-lint", "mypy", "pytest"},
        )
        self.assertEqual(harmful, {"mypy", "pytest"})
        self.assertEqual(stricter, set())

    def test_local_not_ci_is_stricter_only(self) -> None:
        harmful, stricter = compute_drift(
            precommit_kinds={"ruff-lint", "gitleaks"},
            ci_kinds={"ruff-lint"},
        )
        self.assertEqual(harmful, set())
        self.assertEqual(stricter, {"gitleaks"})

    def test_perfect_mirror_no_drift(self) -> None:
        harmful, stricter = compute_drift(
            precommit_kinds={"ruff-lint", "ruff-format", "mypy"},
            ci_kinds={"ruff-lint", "ruff-format", "mypy"},
        )
        self.assertEqual(harmful, set())
        self.assertEqual(stricter, set())


class ThisRepoHasNoDrift(unittest.TestCase):
    """The ingest-platform config must mirror ALL its CI-enforced kinds — this
    is the gate running against the very repo that ships it. The gate is wired
    UNSCOPED (every workflow), so docs.yml's actionlint is in scope alongside
    ci.yml's ruff/mypy/pytest; the pre-commit config mirrors all of them."""

    def test_precommit_mirrors_all_ci_workflows(self) -> None:
        precommit = _REPO_ROOT / ".pre-commit-config.yaml"
        wf_dir = _REPO_ROOT / ".github" / "workflows"
        self.assertTrue(precommit.is_file(), "repo must have a pre-commit config")
        self.assertTrue(wf_dir.is_dir(), "repo must have .github/workflows/")
        ci_paths = sorted(wf_dir.glob("*.y*ml"))
        self.assertTrue(ci_paths, "repo must have at least one workflow")
        harmful, _ = check_repo(precommit, ci_paths)
        self.assertEqual(
            harmful,
            set(),
            f"pre-commit must mirror every CI workflow; missing locally: {sorted(harmful)}",
        )


if __name__ == "__main__":
    unittest.main()
