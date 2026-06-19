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

## Git hooks (required)

This repo mirrors its CI checks locally via [pre-commit](https://pre-commit.com/).
After cloning, install BOTH hook stages once:

```bash
pre-commit install                       # commit-stage checks
pre-commit install --hook-type pre-push  # push-stage checks
```

- **Commit stage** runs: `ruff-format` and `ruff-lint` (with `--fix`) over
  `src/ workers/ tests/`, plus `actionlint` over the workflows.
- **Pre-push stage** runs: `pip-audit` (dependency-vulnerability scan, mirroring
  `ci.yml`'s `security-audit` job — exports the frozen deps and runs
  `pip-audit --strict --desc`), then `mypy --strict` over `src/`, then the
  `pytest` suite (unit only — `-m "not integration"`, so no Docker daemon is
  needed locally).

These mirror `.github/workflows/ci.yml` so failures surface locally before a PR
(org-wide local⇄CI parity, noorinalabs-main#684). Never bypass with
`--no-verify`. If `pre-commit install` "cowardly refuses" because
`core.hooksPath` is set, run `git config --unset core.hooksPath` first.
