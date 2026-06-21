"""Tests for pre_commit_ci_sync — the pre-commit <-> CI drift gate (#327).

Verifies:
  1. Canonical kind extraction from both pre-commit configs and CI workflows.
  2. The drift direction that gates: CI-enforced-but-not-local is harmful;
     local-but-not-CI is stricter-local (informational, never a gate fail).
  3. ruff-format vs ruff-lint are not conflated.
  4. The real parent (noorinalabs-main) config mirrors its CI kinds (no drift).
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

# Helper lives at .claude/lib/pre_commit_ci_sync.py; test is at
# .claude/lib/tests/test_*.py. parent.parent reaches the lib root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pre_commit_ci_sync import (  # noqa: E402
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

    def test_frontend_kinds(self) -> None:
        cfg = """
repos:
  - repo: local
    hooks:
      - id: eslint
      - id: typecheck
        entry: tsc --noEmit
      - id: prettier
"""
        kinds = kinds_from_precommit(cfg)
        self.assertIn("eslint", kinds)
        self.assertIn("typescript", kinds)
        self.assertIn("prettier", kinds)

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

    def test_uses_actions_detected(self) -> None:
        wf = """
jobs:
  scan:
    steps:
      - uses: gitleaks/gitleaks-action@v2
"""
        self.assertIn("gitleaks", kinds_from_ci(wf))

    def test_ruff_format_line_not_counted_as_lint(self) -> None:
        # A format-only line must NOT register ruff-lint.
        kinds = kinds_from_ci("      - run: ruff format --check .\n")
        self.assertIn("ruff-format", kinds)
        self.assertNotIn("ruff-lint", kinds)


class CspellKindClassification(unittest.TestCase):
    """noorinalabs-main#684: the spell-check blind spot. docs.yml's
    `Spellcheck (cspell)` job must classify as the `cspell` kind on EITHER
    expression — the `streetsidesoftware/cspell-action` `uses:` ref, the
    bundled-CLI `cspell` step/run, or the generic `spellcheck` word — and a
    pre-commit cspell hook must classify too, so a CI spell gate with no local
    mirror produces harmful drift instead of silence."""

    def test_cspell_action_uses_ref_classified(self) -> None:
        wf = """
jobs:
  spellcheck:
    name: Spellcheck (cspell)
    steps:
      - name: cspell
        uses: streetsidesoftware/cspell-action@de2a73e # v8.4.0
"""
        self.assertIn("cspell", kinds_from_ci(wf))

    def test_cspell_cli_run_step_classified(self) -> None:
        wf = """
jobs:
  spell:
    steps:
      - run: npx cspell --config .cspell.json "**/*.md"
"""
        self.assertIn("cspell", kinds_from_ci(wf))

    def test_generic_spellcheck_word_classified(self) -> None:
        # A repo that names the step/run with the generic word still registers.
        self.assertIn("cspell", kinds_from_ci("      - run: make spellcheck\n"))

    def test_precommit_cspell_hook_classified(self) -> None:
        cfg = """
repos:
  - repo: https://github.com/streetsidesoftware/cspell-cli
    rev: v8.4.0
    hooks:
      - id: cspell
        name: cspell
"""
        self.assertIn("cspell", kinds_from_precommit(cfg))

    def test_ci_cspell_without_precommit_is_harmful_drift(self) -> None:
        # The exact divergence #684 exists to catch: CI enforces cspell, the
        # pre-commit config does not mirror it.
        wf = """
jobs:
  spellcheck:
    steps:
      - uses: streetsidesoftware/cspell-action@de2a73e
"""
        cfg = """
repos:
  - repo: local
    hooks:
      - id: ruff
"""
        harmful, _ = compute_drift(kinds_from_precommit(cfg), kinds_from_ci(wf))
        self.assertIn("cspell", harmful)

    def test_ci_cspell_with_precommit_mirror_no_drift(self) -> None:
        wf = """
jobs:
  spellcheck:
    steps:
      - uses: streetsidesoftware/cspell-action@de2a73e
"""
        cfg = """
repos:
  - repo: https://github.com/streetsidesoftware/cspell-cli
    rev: v8.4.0
    hooks:
      - id: cspell
"""
        harmful, _ = compute_drift(kinds_from_precommit(cfg), kinds_from_ci(wf))
        self.assertNotIn("cspell", harmful)


class BuildKindTightening(unittest.TestCase):
    """#576: runtime `docker build` / `docker buildx` steps are image-MOVING,
    not a build-QUALITY gate a local pre-commit hook can mirror — they must
    NOT classify as the `build` kind, or any docker-image repo gets a
    permanent un-mirrorable drift. Real build-quality gates still register."""

    def test_docker_buildx_not_build_kind(self) -> None:
        wf = """
jobs:
  publish:
    steps:
      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3
      - name: Build and push
        run: docker buildx build --push -t img:latest .
"""
        self.assertNotIn("build", kinds_from_ci(wf))

    def test_bare_docker_build_not_build_kind(self) -> None:
        self.assertNotIn("build", kinds_from_ci("      - run: docker build -t img .\n"))

    def test_real_build_quality_gates_still_detected(self) -> None:
        # The classifier inspects step lines (run:/uses:/`- ` list items), so
        # the build-quality markers are exercised in those positions.
        for line in (
            "      - name: build-and-validate\n",
            "      - run: ./scripts/build-and-test.sh\n",
            "      - run: npm run build\n",
        ):
            with self.subTest(line=line):
                self.assertIn("build", kinds_from_ci(line))

    def test_docker_publish_workflow_has_no_build_drift(self) -> None:
        # End-to-end: a publish-only workflow (docker buildx) against a config
        # that does NOT mirror a build kind produces NO harmful drift.
        wf = """
jobs:
  publish:
    steps:
      - run: docker buildx build --push -t img .
"""
        cfg = """
repos:
  - repo: local
    hooks:
      - id: ruff
"""
        harmful, _ = compute_drift(kinds_from_precommit(cfg), kinds_from_ci(wf))
        self.assertNotIn("build", harmful)


class MultiLineRunBlockScanning(unittest.TestCase):
    """#577: tools invoked on a CONTINUATION line of a multi-line `run: |` /
    `run: >` block must be classified — else the gate has a silent blind spot
    where a future `ruff`/`mypy` expressed multi-line drops out of the mirror
    check."""

    def test_pipe_block_continuation_lines_classified(self) -> None:
        wf = """
jobs:
  security-audit:
    steps:
      - name: audit
        run: |
          uv sync
          uv run pip-audit
"""
        self.assertIn("pip-audit", kinds_from_ci(wf))

    def test_multiple_tools_in_one_block(self) -> None:
        wf = """
jobs:
  checks:
    steps:
      - name: lint+type+test
        run: |
          ruff check .
          mypy src/
          pytest -q
"""
        kinds = kinds_from_ci(wf)
        self.assertEqual(
            kinds & {"ruff-lint", "mypy", "pytest"},
            {"ruff-lint", "mypy", "pytest"},
        )

    def test_folded_block_scalar_also_scanned(self) -> None:
        # `run: >` (folded) and chomping indicators (`|-`) open a block too.
        wf = """
jobs:
  x:
    steps:
      - run: >-
          mypy src/
"""
        self.assertIn("mypy", kinds_from_ci(wf))

    def test_block_ends_at_dedented_sibling_key(self) -> None:
        # A less-indented sibling key (here `uses:` + its `with:`) closes the
        # block — its `fetch-depth` body must not leak a spurious classification
        # and the block's own `ruff check` is still caught.
        wf = """
jobs:
  x:
    steps:
      - name: a
        run: |
          ruff check .
      - name: b
        uses: actions/checkout@v4
        with:
          fetch-depth: 0
"""
        kinds = kinds_from_ci(wf)
        self.assertIn("ruff-lint", kinds)

    def test_single_line_run_unaffected(self) -> None:
        # Regression: the common single-line `run:` form still classifies.
        wf = """
jobs:
  x:
    steps:
      - run: ruff check .
      - run: mypy .
"""
        kinds = kinds_from_ci(wf)
        self.assertEqual(kinds & {"ruff-lint", "mypy"}, {"ruff-lint", "mypy"})

    def test_comment_in_block_not_classified(self) -> None:
        # A commented continuation line inside the block is not a real
        # invocation and must not register.
        wf = """
jobs:
  x:
    steps:
      - run: |
          # pytest is mentioned here but commented out
          echo hi
"""
        self.assertNotIn("pytest", kinds_from_ci(wf))


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


class RealParentRepoHasNoDrift(unittest.TestCase):
    """The parent noorinalabs-main config must mirror its CI kinds — this is
    the gate running against the very repo that ships it."""

    def test_parent_precommit_mirrors_parent_ci(self) -> None:
        precommit = _REPO_ROOT / ".pre-commit-config.yaml"
        ci = _REPO_ROOT / ".github" / "workflows" / "ci.yml"
        self.assertTrue(precommit.is_file(), "parent must have a pre-commit config")
        self.assertTrue(ci.is_file(), "parent must have ci.yml")
        harmful, _ = check_repo(precommit, [ci])
        self.assertEqual(
            harmful,
            set(),
            f"parent pre-commit must mirror CI; missing locally: {sorted(harmful)}",
        )


if __name__ == "__main__":
    unittest.main()
