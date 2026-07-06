# Streaming pipeline — E2E verification (P8W24, ip#120 / main#667)

**Author:** Bjørn Henriksen (DevOps Architect) · **Date:** 2026-07-05 · **Wave:** Phase 8 Wave 24 (verify-and-close)

This is the **operational proof** for phase-8 end-state criterion #4 (`main#667`):
the streaming pipeline brings up one-command and streams **incrementally**
(offsets advancing, not a batch load), **repeatably**. The pipeline itself was
already built, unit-tested, and documented on `main` (4 workers +
`workers/lib`, `docker-compose.yaml`, `Makefile pipeline`, `RUNBOOK.md`,
`tests/integration/test_kafka_worker_e2e.py`); this wave is verification, plus
the fixes that verification surfaced.

## Environment

| Capability | This host | Notes |
|---|---|---|
| `uv` / Python 3.14 | present (uv 0.10.10, py 3.14.3) | worker unit tests + no-Docker E2E run here |
| Docker daemon | **present** (29.5.3) | real Kafka + MinIO + Neo4j integration suite runs here |
| ML group (`sentence-transformers`/`faiss`/`transformers`/`torch`) | absent | dedup/enrich exercise their documented graceful-degradation path — plumbing proven regardless |
| Staging box / `deploy` infra | **not present** | the profiled `--profile pipeline` + `deploy-pipeline-stg` bring-up leg is verified as-documented, not executed live |

Everything below ran in a worktree off the wave branch `deployments/phase-8/wave-24`.

## Commands run and evidence

### 1. Worker unit suite (no infra)

```
ENVIRONMENT=test make test-workers
→ 91 passed, 1 skipped in 1.59s
```

The 1 skip is the real-dedup near-duplicate test (needs the ML group). Includes
the idempotency + canonical-id coverage relied on below
(`test_chain_is_idempotent_on_replay`, `test_handle_one_is_idempotent_on_batch_id`,
`test_hadith_id_matches_batch_loader_no_doubled_corpus`).

### 2. No-Docker first-light E2E (`scripts/e2e_pipeline_run.py`) — run twice

```
uv run python scripts/e2e_pipeline_run.py          # run 1
uv run python scripts/e2e_pipeline_run.py --json   # run 2
```

Both runs, byte-identical end state, exit 0:

```
Per-hop traversal:
  dedup      pipeline.raw.landed      → pipeline.dedup.done      (in 1→out 1, dlq 0, +2 objects)
  enrich     pipeline.dedup.done      → pipeline.enrich.done     (in 1→out 1, dlq 0, +2 objects)
  normalize  pipeline.enrich.done     → pipeline.normalize.done  (in 1→out 1, dlq 0, +7 objects)
  ingest     pipeline.normalize.done  → (terminal → Neo4j)       (in 1→out 0, dlq 0)
DLQ entries: 0
Graph end state:  Hadith 3 · Collection 2 · Narrator 5 · Chain 3 · Grading 2
                  APPEARS_IN 3 · TRANSMITTED_TO 4 · NARRATED 2 · GRADED_BY 2
```

Identical counts across two fresh runs ⇒ **repeatable / idempotent at the E2E
level**. The chain runs production code at every hop (real processors, real
`WorkerRunner`, real `ObjectStore`, real `PipelineMessage` pointers, real ingest
Cypher MERGE); only the transport (in-memory broker), object store (in-memory
S3) and terminal sink (in-process `CapturingNeo4jDriver`) are substituted for
the missing infra.

### 3. Real-Kafka integration E2E (Docker) — `tests/integration/test_kafka_worker_e2e.py`

```
ENVIRONMENT=test uv run pytest tests/integration/test_kafka_worker_e2e.py -v -m integration
→ 3 passed in 100.08s   (Kafka + MinIO + Neo4j via testcontainers)
```

This drives the workers **through real Kafka topics** (`KafkaConsumer` group
subscription across `pipeline.dedup.done → pipeline.enrich.done →
pipeline.normalize.done`), a real MinIO object hand-off, and a terminal MERGE
into a real Neo4j; a companion test builds each stage's production
`main.build_runner()` against the live containers (the ingest entrypoint runs
`driver.verify_connectivity()`).

> **Finding fixed in this PR.** On first run this suite was **red** —
> `test_enrich_normalize_ingest_e2e_through_kafka_topics` asserted the hadith
> id `hdt:sunnah:h-1`, the pre-#63/#72 `hdt:<corpus>:<source_id>` rule. The
> production path correctly MERGEd `hdt:h-1` (fixes #63/#72 route both the
> streaming normalize path and the batch loader through the single
> `hadith_node_id()` helper, which drops the corpus double-prefix). The test's
> expectation was **stale** — it was authored before #63/#72 and, because CI
> runs `-m "not integration"`, the Docker-gated assertion was never re-checked
> when the id rule changed. Fixed by asserting `hadith_node_id("h-1")` (the
> single source of truth) so it cannot drift again. Suite is green after the fix.

### 4. Live offset-advance observation (real Kafka + MinIO + Neo4j)

Drove the **full four-stage chain** (dedup → enrich → normalize → ingest) with a
realistic corpus-prefixed sunnah fixture, reading each pipeline topic's Kafka
**log-end offset** after every hop. Offsets advance stage-by-stage — this is
per-message streaming, not a monolithic batch load:

```
                            raw.landed  dedup.done  enrich.done  normalize.done
  seed → raw.landed              1           0           0            0
  after dedup                    1           1           0            0
  after enrich                   1           1           1            0
  after normalize                1           1           1            1
  after ingest (terminal)        1           1           1            1
```

Terminal ingest MERGEd into the live Neo4j; the **canonical** id landed
(no double-prefix), content queryable:

```
hdt:sunnah:bukhari:1:1  found=True
  matn = "Actions are judged by intentions."   sect = "sunni"   collection = "col:bukhari"
Live Neo4j node counts: Hadith 3 · Collection 2 · Narrator 5 · Chain 3 · Grading 2
```

## How progress / replay-safety actually works (for on-call accuracy)

- **Incremental streaming** is the topic-per-stage log progression above: each
  worker consumes one pointer message from its input topic and produces one to
  the next stage's topic, so the downstream log-end offset advances per message.
  This is structurally distinct from the one-shot `make pipeline` batch path.
- **Durable progress / idempotency** is the **Postgres `worker_checkpoint`**
  (`workers/lib/checkpoint_pg.py`), not Kafka committed offsets. The production
  consumers run `enable_auto_commit=False` with no manual commit; replay-safety
  comes from the runner's `seen(batch_id)` / `mark(batch_id)` guard with
  send-before-mark ordering (#43) — a re-delivered batch is skipped
  (`idempotent_skip`), a downstream send that fails before `mark` is safely
  re-sent on restart. Covered by `test_chain_is_idempotent_on_replay` and
  `test_send_failure_after_mark_does_not_record_checkpoint_*`.

## One-command bring-up paths — audited against `main`

| Path | Status |
|---|---|
| No-Docker first-light: `uv run python scripts/e2e_pipeline_run.py` | **Verified here** — one command, green, exit 0 |
| Local-dev compose: infra from `noorinalabs-deploy` + `docker compose up --build` (4 worker services) | Compose + all 4 services present and correct; infra half lives in the sibling deploy repo |
| `make pipeline` (batch end-to-end) | Target present and correct (`acquire→parse→resolve→load→enrich`) |
| Staging: publish image (`ghcr-publish.yml`) → gated `--profile pipeline up -d` → `deploy-pipeline-stg` dispatch (`deploy#443`, CLOSED) | Workflow present here; `deploy#443` **CLOSED**; documented commands consistent — **live execution needs the staging box (not in this env)** |

RUNBOOK drift corrected in this PR: the "Known divergence (double-prefix)" note
and its `hdt:sunnah:sunnah:bukhari:1:1` cypher example were **stale** — #63/#72
resolved the double-prefix. Updated to reflect the resolution + the canonical
`hdt:sunnah:bukhari:1:1` id, and cross-linked to this report.

## Verdict on `main#667` "done when" items

| # | Item | Verdict |
|---|---|---|
| 1 | One-command bring-up per RUNBOOK | **Verified here** for the no-Docker first-light path (green, one command) and the local/staging compose paths **audited as-documented** (compose + all 4 workers + `ghcr-publish.yml` present; `deploy#443` closed). Full staging `--profile pipeline` bring-up **needs the staging box** — not present in this env. |
| 2 | Incremental streaming E2E (offsets advance, not batch) | **Verified here, live.** Real-Kafka integration suite green (3 passed) + a live log-end-offset-advance observation `raw.landed→dedup.done→enrich.done→normalize.done` (each 0→1) + canonical node/edge counts MERGEd into a real Neo4j. |
| 3 | Repeatable runbook / idempotent on replay | **Verified here.** No-Docker E2E reproduced byte-identical across two fresh runs; unit replay-idempotency (`test_chain_is_idempotent_on_replay`, checkpoint guard) green; real-Kafka traversal green. |

**Recommendation:** the pipeline is proven **repeatable, incremental,
offset-based, and one-command** as far as this environment allows — every leg
runnable without the staging box is green. The single remaining leg is the live
`--profile pipeline` bring-up on the **staging box** (the `deploy#443` dispatch),
which requires stg infra + the stg-gate and cannot be exercised here. Once that
staging bring-up is executed and its offsets/graph observed, `main#667` can be
closed. This report + the now-green integration suite constitute the evidence
block for items 1 (dev/local + audit), 2, and 3.
