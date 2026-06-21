# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**noorinalabs-isnad-ingest-platform** is the standalone data-ingestion platform for the isnad-graph project. It is the **processing pipeline** half of the data split: a chain of Kafka-driven streaming workers that consume staged batches from S3-compatible object storage (Backblaze B2 in prod, MinIO for local dev), **dedup → enrich → normalize → graph-load** them, and write the result into Neo4j.

Each stage is a worker that consumes one Kafka topic, reads a batch from object storage, processes it, writes results back to storage, and publishes a pointer message on the next stage's topic. The message contract is **pointer-based** (the payload carries a `b2_path` to the Parquet batch, not the records themselves); all failures route to a dead-letter topic (`pipeline.dlq`).

- **Acquisition vs. platform split:** data *acquisition* (scrapers, API connectors, downloaders that land files in B2 `raw/`) lives in the sibling `noorinalabs-data-acquisition` repo. This repo owns the *processing* pipeline only. See `.claude/memory/project_data_pipeline_architecture.md`.
- **`src/` vs. `workers/`:** `src/` holds the processing libraries (`resolve`, `enrich`, `graph`, `parse`, `models`, `pipeline`) plus the FastAPI metrics/admin API; `workers/` holds the thin Kafka-driven streaming workers that build on those libraries. The worker topology and message contract are documented in [`workers/README.md`](workers/README.md); operational procedures are in [`RUNBOOK.md`](RUNBOOK.md).

## Tech Stack

- **Python ≥ 3.14** — managed with **uv** (package manager + lockfile)
- **Apache Kafka (KRaft mode, no ZooKeeper)** — `kafka-python` client; topic-per-stage streaming topology
- **Object storage** — Backblaze B2 in prod, **MinIO** for local dev (S3-compatible, same code path); `boto3` client
- **Neo4j 5.x** (`neo4j` driver) — terminal graph store; **PostgreSQL** (`psycopg` v3) — worker checkpoints / hadith metadata
- **PyArrow / pandas** — Parquet staging batches
- **Pydantic v2** + **pydantic-settings** — frozen models and config
- **FastAPI / uvicorn** — pipeline metrics + admin (reset / reprocess) API under `src/api/`
- **structlog** — JSON structured logging
- **testcontainers** (`[neo4j,postgres,kafka,minio]`) — integration-test harness spinning real infra in Docker
- **ML group (optional, `--group ml`):** sentence-transformers, faiss-cpu, transformers, torch, camel-tools — used by the dedup/enrich stages

## Build & Development Commands

The `Makefile` is the canonical entry point; all targets run pinned tools via `uv run`.

```bash
# Setup
make setup          # uv sync --group ml  (installs deps incl. the ML group for dedup)
make setup-hooks    # install pre-commit + commit-msg git hooks

# Pipeline stages (also exposed as the `isnad-ingest` CLI)
make acquire        # Phase 1: download data sources
make parse          # Phase 1: parse raw data into staging Parquet
make resolve        # Phase 2: entity resolution (NER + disambiguation + dedup)
make load           # Phase 3: load graph into Neo4j
make enrich         # Phase 4: metrics, topics, historical overlay
make pipeline       # full end-to-end run (acquire → parse → resolve → load → enrich)

# Quality (mirror CI — see § Code Conventions)
make lint           # ruff check src/ workers/ tests/
make format         # ruff format src/ workers/ tests/
make typecheck      # mypy src/
make test           # pytest (full suite)
make check          # ruff check + ruff format --check + mypy + pytest -m "not integration"

# Streaming workers
make test-workers   # worker unit tests (in-memory fakes; no Docker/Kafka)
make build-workers  # build all 4 per-stage worker Docker images
make test-integration   # integration tests — requires a Docker daemon (testcontainers)

# Data ops
make validate           # data-quality validation (strict, JSON report)
make validate-staging   # staging Parquet schema validation (warn mode)
make clean              # remove staging data + tool caches
```

Local worker smoke test via Docker Compose (`docker-compose.yaml` ships the 4 worker services; Kafka/MinIO/Neo4j come from `noorinalabs-deploy`):

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export PIPELINE_BUCKET=noorinalabs-pipeline
export S3_ENDPOINT_URL=http://minio:9000
docker compose up --build
```

## Architecture

### Streaming-worker topology (`workers/`)

| Worker | Consumes | Produces | Ported from |
|---|---|---|---|
| `dedup-worker` | `pipeline.raw.new` | `pipeline.dedup.done` | `src/resolve/` |
| `enrich-worker` | `pipeline.dedup.done` | `pipeline.enrich.done` | `src/enrich/` |
| `normalize-worker` | `pipeline.enrich.done` | `pipeline.norm.done` | (new) |
| `ingest-worker` | `pipeline.norm.done` | Neo4j (terminal) | `src/graph/` |

All failures route to `pipeline.dlq`. Each worker is its own container; the shared consume/fetch/process/write/publish/DLQ loop, the pointer-message schema, the S3 client, checkpointing, and metrics live in `workers/lib/`. A single multi-stage image (top-level `Dockerfile`) is the artifact the deployed stack pulls; the stage is selected at start time by the compose service's `command:`. The per-stage `workers/<stage>/Dockerfile` images remain for the self-contained local-dev compose.

### Processing libraries (`src/`)

| Path | Purpose |
|------|---------|
| `src/acquire/`, `src/parse/` | Source downloaders + raw→Parquet parsers (Phase 1) |
| `src/resolve/` | Entity resolution — NER, disambiguation, dedup (Phase 2) |
| `src/graph/` | Parquet → Neo4j node/edge loaders (Phase 3) |
| `src/enrich/` | Graph metrics, topic classification, historical linking (Phase 4) |
| `src/pipeline/` | Stage orchestration, manifest/audit, **reset** + **reprocess** admin logic |
| `src/api/` | FastAPI app — pipeline metrics + admin reset/reprocess routers (auth-gated) |
| `src/models/` | Pydantic v2 domain models (hadith, narrator, chain, grading, …) |
| `src/utils/` | Arabic normalization, Neo4j/PG clients, structlog setup |

### Tests (`tests/`)

- `tests/test_*/` — per-module unit tests; `tests/workers/` — worker unit tests using **in-memory fakes** for Kafka/S3 (no infra).
- `tests/integration/` — real-infra E2E via **testcontainers** (Neo4j, Postgres, Kafka, MinIO), marked `@pytest.mark.integration` and excluded from the default/CI unit run.

## Code Conventions

- **ruff** — `line-length = 100`, rule sets `E, F, I, UP, PD`; pinned to **v0.15.11** (org-canonical pin — keep in lockstep with sibling repos + CI). `ruff format` is the formatter.
- **mypy** — `strict = true` with the `pydantic.mypy` plugin; type-checks `src/`.
- **pytest** — markers: `integration` (needs Docker), `ml` (needs the ML group), `serial` (must run sequentially). CI and the local push hook run `-m "not integration"`.
- **Pydantic v2** models, frozen where they represent immutable records.
- The full check-set is mirrored locally by `.pre-commit-config.yaml` (see § Local Hooks).

### CI workflows

- **`ci.yml`** — `lint-and-typecheck` (ruff check + ruff format --check over `src/ workers/ tests/`, mypy `src/`), `security-audit` (`pip-audit --strict` over the frozen deps), `test` (`pytest -m "not integration"`), and `precommit-ci-sync` (the pre-commit ⇄ CI sync-drift gate).
- **`docs.yml`** — markdown lint, cspell, lychee internal-link check, YAML/JSON config syntax, and actionlint over the workflows.
- **`ghcr-publish.yml`** — builds and publishes the multi-stage worker image to GHCR.

> **Known-red gate:** `security-audit` (`pip-audit --strict`) can fail on newly-disclosed transitive CVEs unrelated to a given change (advisory-DB drift). A red `security-audit` that your change did not cause is **not** something to fold a dep bump into an unrelated PR over — flag it; fix it in a dedicated deps PR. Never `--no-verify`, never force-push past a check you did cause.

### Local Hooks (pre-commit + pre-push)

`.pre-commit-config.yaml` mirrors `ci.yml` + `docs.yml` so a local commit/push fails fast instead of at PR time (org-wide local⇄CI parity, noorinalabs-main#684). Install BOTH stages once:

```bash
pre-commit install                       # commit-stage: ruff-format, ruff-lint, actionlint
pre-commit install --hook-type pre-push  # push-stage: pip-audit, mypy --strict, pytest (unit only)
```

`.claude/lib/pre_commit_ci_sync.py` is the CI gate that fails the build if a check CI enforces is not mirrored in the pre-commit config. Run it locally with `python3 .claude/lib/pre_commit_ci_sync.py .`.

## Configuration

Copy `.env.example` to `.env` for local runs. Workers read `KAFKA_BOOTSTRAP_SERVERS`, `PIPELINE_BUCKET`, `S3_ENDPOINT_URL` (set for MinIO/local, unset for B2 prod), and `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`; the `ingest-worker` additionally needs `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`.

## Team Workflow

> **Cross-repo session-team note:** The team structure described below is the **per-repo team** — operative when a session is opened isolated in this repo for repo-only work.
>
> When work is orchestrated from the parent `noorinalabs-main` (the common case — wave kickoff, cross-repo features, wave-coordinated bug fixes), all spawned agents — regardless of which repo they edit — join the single `noorinalabs` session team. The per-repo roster below still governs **commit identity, domain ownership, and reviewer pairing**, but the team-creation surface lives in the orchestrator session, not here.
>
> See `noorinalabs-main/CLAUDE.md` § "Session team architecture" and `noorinalabs-main/.claude/team/charter/agents.md` § "Single-Leader Constraint" for the delegation pattern.

**All work MUST be executed through the simulated team structure.** No work begins without spawning the team.

> **Note:** The authoritative team config (charter, roster, hooks, skills) lives in the parent repo (`noorinalabs-main/.claude/`). This repo retains a local copy for agents working within noorinalabs-isnad-ingest-platform.

- **Charter & rules:** `.claude/team/charter.md`
- **Active roster:** `.claude/team/roster/`
- **Roster lookup (hooks):** `.claude/team/roster.json`
- **Feedback log:** `.claude/team/feedback_log.md`

### Team Composition

| Role | Level | Name | File |
|------|-------|------|------|
| Manager | Senior VP (Executive) | Adaeze Okonkwo | `roster/manager_adaeze.md` |
| System Architect | Partner | Sayed Reza | `roster/architect_sayed.md` |
| Tech Lead | Staff | Petra Vidović | `roster/tech_lead_petra.md` |
| Data Lead | Staff | Camila Restrepo | `roster/data_lead_camila.md` |
| DevOps Architect | Staff | Bjørn Henriksen | `roster/devops_architect_bjorn.md` |
| Engineer | Principal | Yusuke Inoue | `roster/engineer_yusuke.md` |
| Engineer | Senior | Léopold Mbongo | `roster/engineer_leopold.md` |
| Data Engineer | Senior | Imelda Santos | `roster/data_engineer_imelda.md` |
| DevOps Engineer | Senior | Fatima Bensalah | `roster/devops_engineer_fatima.md` |
| Security Engineer | Senior | Kalinda Ranasinghe | `roster/security_engineer_kalinda.md` |
| QA Engineer | Senior | Tomás Carvalho | `roster/qa_engineer_tomas.md` |

### Key Rules

- **Commit identity:** Each team member commits using per-commit `-c` flags with their name and `parametrization+{FirstName}.{LastName}@gmail.com` email — **never** set global/repo git config. See `.claude/team/charter.md` § Commit Identity and `.claude/team/roster.json` for the full table.
- **Worktrees** are the preferred isolation method for all code-writing agents.
- Manager spawns team members, creates stories/AC, and owns timelines.
- Feedback flows up and down; severe feedback triggers fire-and-replace.
- Team evolves toward steady state of minimal negative feedback.

## Developer Tooling & Orchestration

- **gh-cli** is installed and available from the terminal
- **SSH access** is enabled from the terminal
- **GitHub Projects** — project/feature tracking and board management
- **GitHub Issues** — story/task/bug tracking (created by Manager, assigned to team members)
- **GitHub Actions** — CI/CD pipelines, automated tests, linting, deployment
- These three (Projects, Issues, Actions) are the **core orchestration layer** — do not introduce alternative tools for these concerns
- **Branching strategy:** Feature branches named `{FirstInitial}.{LastName}/{IIII}-{issue-name}` (e.g., `S.Reza/0740-add-claude-md`) merged to `main` via PR. This repo follows **GitHub Flow** (feature branches off `main`, no long-lived phase/wave branches).

## Project Memory

Project memory is **version-controlled in the repo** at `.claude/memory/`, not in the user-space auto-memory directory — so a developer who pulls a branch gets the memory with it, zero per-machine setup. The index below is auto-loaded into every session via this committed import:

@.claude/memory/MEMORY.md

`MEMORY.md` is the always-loaded index (one line per memory); the individual topic files in `.claude/memory/*.md` are read on demand. To record a memory, create or edit `.claude/memory/<kebab-slug>.md` with frontmatter (`name`, `description`, `metadata.type`), add a one-line pointer to `MEMORY.md`, and commit it so it travels with the branch.
