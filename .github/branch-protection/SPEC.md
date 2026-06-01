# Branch Protection — noorinalabs-isnad-ingest-platform (P3 end-state #4, main#322)

Phase-3 end-state criterion #4 (`noorinalabs-main#322`): **CI failures block all
merges** on every repo's default branch, org-wide — enforced server-side by
GitHub, not only by the Hook 4 comment-gate. This directory carries the
canonical ruleset for this repo's `main`:

| File | Purpose |
|------|---------|
| `ruleset-main.json` | The repository ruleset payload (GitHub REST `/rulesets`). |
| `apply-ruleset.sh`  | Owner/admin-gated apply + read-back-verify. Idempotent (create-or-update). |
| `SPEC.md`           | This document — the shape and the why. |

This is the ingest-platform's adoption of the parent-canonical spec
(`noorinalabs-main` charter `pull-requests.md` § *Org-Wide Branch Protection +
Admin-Merge Exceptions*), modeled on the W13 live pilot
(`noorinalabs-data-acquisition`, ruleset id `17091263`).

## Application status

The **spec + apply script** land in this PR (W14, `Refs noorinalabs-main#322`).
The actual **apply is owner/admin-gated** and is a **post-merge step**:

1. Creating a repository ruleset requires repo-admin permission, which the agent
   `gh` principal (`parametrization`) does not hold for this purpose.
2. Applying default-branch protection while a wave-branch PR is in flight can
   block our own merges, so the apply runs from a window with **no in-flight
   default-branch merge** — post-wave-wrapup is the safe window.

So #322 is **met for this repo only when the owner has run `apply-ruleset.sh`
and read-back-verified the ruleset on `main`.** `#322` stays OPEN as the
org-wide rollout tracker until all 8 default branches carry the protection.

## The ruleset shape (and why)

A **repository ruleset** targeting `~DEFAULT_BRANCH`, `enforcement: active`:

- **`pull_request` with `required_approving_review_count: 0`** — the load-bearing
  decision. GitHub's "require approvals" counts **formal** GitHub PR reviews,
  which our team structurally cannot produce: the `gh` auth principal IS the PR
  author (`parametrization`), so a formal self-approval **422s**, and our review
  discipline runs on **issue-comment verdicts** validated by Hook 4
  (`validate_pr_review`), not formal reviews. A naive "require 1 approval" rule
  would **deadlock every merge**. Reviewer-count enforcement stays with Hook 4.
- **`required_status_checks` (strict)** — ingest-platform has **unconditional PR
  CI** (`ci.yml` has no `paths:` filter), so the ruleset hard-requires its gate
  **job-name** contexts:

  | Context | Source job |
  |---------|-----------|
  | `lint-and-typecheck` | `ci.yml` → `lint-and-typecheck` (ruff lint + format + mypy) |
  | `security-audit` | `ci.yml` → `security-audit` (pip-audit) |
  | `test` | `ci.yml` → `test` (pytest, `-m "not integration"`) |
  | `precommit-ci-sync` | `ci.yml` → `precommit-ci-sync` (pre-commit ⇄ CI sync-drift gate) |

  These are the four unconditional `ci.yml` jobs. The `docs.yml` jobs
  (markdownlint / spellcheck / linkcheck / config-lint / actionlint) are
  **deliberately NOT required** here: `docs.yml` is `paths:`-filtered, so its
  checks do not run on a code-only PR and requiring them would block every
  non-docs merge on a never-arriving status. **Re-confirm all contexts at apply
  time** against live check-runs — job names can change:
  `gh api repos/<repo>/commits/<default-sha>/check-runs --jq '.check_runs[].name'`.
- **`deletion` + `non_fast_forward`** — no force-push / branch-delete on `main`.
- **`bypass_actors`: Repository-admin (`actor_id: 5`, `bypass_mode: always`)** —
  keeps the orchestrator's `--admin` wave→main wrapup merges and the charter
  single-reviewer / doc-sweep / emergency exceptions working. The GitHub-side
  bypass is mirrored on the operator side by the hook-validated
  `ADMIN_MERGE_EXCEPTION` gate (`validate_pr_ci_status`), which **audits** every
  `--admin` merge to the Annunaki trail — defense in depth: the ruleset covers
  UI/external/batch-loop merges, the hook covers `gh pr merge` and names the
  exceptions.

## How to apply (owner)

```bash
# From a window with NO in-flight default-branch merge (post-wave-wrapup):
.github/branch-protection/apply-ruleset.sh            # create or update
DRY_RUN=1 .github/branch-protection/apply-ruleset.sh  # preview only

# Then read-back-verify the detail (contexts + bypass actor):
gh api repos/noorinalabs/noorinalabs-isnad-ingest-platform/rulesets \
  --jq '.[] | select(.name|startswith("Protect main")) | .id'
gh api repos/noorinalabs/noorinalabs-isnad-ingest-platform/rulesets/<id>
```
