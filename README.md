# noorinalabs-isnad-ingest-platform

Pipeline processing — Kafka workers for dedup/enrich/normalize/graph-load.

Standalone ingestion platform for the isnad-graph project: streaming workers
consume staged batches from S3-compatible storage (B2 / MinIO), dedup, enrich,
and normalize them, then load the result into Neo4j. The worker topology and
message contract live in [`workers/README.md`](workers/README.md); operational
procedures are in [`RUNBOOK.md`](RUNBOOK.md).

## What's here

- `src/` — processing libraries (resolve, enrich, graph load) the workers build on.
- `workers/` — the Kafka-driven streaming workers and their shared runtime.
- `docker-compose.yaml` — local Kafka / MinIO / Neo4j stack for development.

## Local hooks (required)

This repo ships a `.pre-commit-config.yaml` that **mirrors the complete CI
check-set** (`.github/workflows/ci.yml` + `docs.yml`) so a local commit/push
fails fast instead of surfacing a lint/type/test/spell error only after a PR is
opened (Phase-3 end-state criterion #6 / noorinalabs-main#327). After cloning,
install BOTH hook stages once:

```bash
pre-commit install                       # commit-stage hooks
pre-commit install --hook-type pre-push  # push-stage hooks
```

- **Commit stage (every commit)** — fast, no Docker daemon:
  - `ruff-format` + `ruff-lint` (with `--fix`) over `src/ workers/ tests/`
  - `actionlint` over the workflows (mirrors `docs.yml`)
  - `cspell` over the authored-prose docs (mirrors `docs.yml`'s spellcheck job)
  - `dockerfile-base-pin` — every Dockerfile `FROM` (the top-level multi-worker
    image + the four per-stage worker images) must be digest-pinned
    (`image:tag@sha256:<digest>`) and carry the matching `apt-get -y upgrade`
    (charter § Base Image Pinning, noorinalabs-main#735 / #744)
- **Pre-push stage (every push)** — the heavier checks:
  - `pip-audit` — dependency-vulnerability scan, mirroring `ci.yml`'s
    `security-audit` job (exports the frozen deps and runs
    `pip-audit --strict --desc`)
  - `mypy --strict` over `src/`
  - the `pytest` suite (unit only — `-m "not integration"`, so no Docker daemon
    is needed locally)

**Sync-drift gate:** the `precommit-ci-sync` CI job
(`.claude/lib/pre_commit_ci_sync.py`) fails the build if a check CI enforces
(ruff / mypy / pytest / actionlint / cspell / `dockerfile-base-pin` / …) is NOT
mirrored in `.pre-commit-config.yaml`, so the local⇄CI mirror cannot rot. Run it
locally with `python3 .claude/lib/pre_commit_ci_sync.py .`.

These mirror CI so failures surface locally before a PR (org-wide local⇄CI
parity, noorinalabs-main#684). **Never** bypass with `--no-verify`, and never
push or merge with a known-failing check. If `pre-commit install` "cowardly
refuses" because `core.hooksPath` is set, run `git config --unset core.hooksPath`
first.
