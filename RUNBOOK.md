# noorinalabs-isnad-ingest-platform — Operational Runbook

Pipeline-processing repo. Four Kafka workers (KRaft mode, no ZooKeeper)
move batches through B2/MinIO stage prefixes
(`raw → dedup → enriched → normalized → staged`) and finally MERGE into
Neo4j. Status: **scaffold + happy-path wiring complete**; production
cutover is gated on the carry-forward issues at the end of this
document.

This runbook covers the operational procedures for the workers
themselves. Cluster-level Kafka, MinIO, Neo4j, and Postgres runtime
ownership lives in `noorinalabs-deploy`; cross-link below where
relevant.

---

## Table of contents

1. [Topology](#topology)
2. [Local-dev setup (MinIO + KRaft Kafka)](#local-dev-setup-minio--kraft-kafka)
3. [Worker build and run](#worker-build-and-run)
4. [Deploy procedure](#deploy-procedure)
5. [Rollback](#rollback)
6. [Common-failure triage](#common-failure-triage)
7. [On-call escalation](#on-call-escalation)
8. [References](#references)
9. [Known gaps and carry-forwards](#known-gaps-and-carry-forwards)

---

## Topology

| Worker | Consumes | Produces | Output prefix | Sink |
|---|---|---|---|---|
| `dedup-worker` | `pipeline.raw.landed` | `pipeline.dedup.done` | `dedup/{source}/{batch-id}/` | B2/MinIO |
| `enrich-worker` | `pipeline.dedup.done` | `pipeline.enrich.done` | `enriched/{source}/{batch-id}/` | B2/MinIO |
| `normalize-worker` | `pipeline.enrich.done` | `pipeline.normalize.done` | `normalized/{batch-id}/` | B2/MinIO |
| `ingest-worker` | `pipeline.normalize.done` | (terminal) | `staged/{batch-id}/` | Neo4j |

All four workers share `pipeline.dlq` as the dead-letter sink. The
runner (`workers/lib/runner.py`) catches every uncaught exception in
`process()`, builds a `DLQRecord` with the original pointer + traceback,
and publishes to `pipeline.dlq`. This is intentional: a single bad
batch never crashes the consumer loop.

Topic constants are the single source of truth in
`workers/lib/topics.py`. Upstream
producers (e.g. `noorinalabs-data-acquisition`'s `kafka_producer.py`)
import from there — do not hardcode topic names elsewhere.

The pipeline is **pointer-based**: Kafka messages carry a `batch_id`,
`source`, `b2_path`, and `record_count` (see
`workers/lib/message.py`). The actual Parquet payload lives in B2 /
MinIO; messages stay small.

---

## Local-dev setup (MinIO + KRaft Kafka)

The default local-dev story shares infra with `noorinalabs-deploy`:
Kafka, MinIO, and Neo4j come from the deploy compose stack on a shared
Docker network, and this repo's `docker-compose.yaml` brings up only
the workers.

### One-time prerequisites

```bash
# Python 3.14 + uv
uv sync --group ml          # `--group ml` only needed if iterating on dedup
uv run pre-commit install --hook-type pre-commit --hook-type commit-msg
docker --version            # 24+
```

### Bring up infra

The canonical local infra compose lives in `noorinalabs-deploy`. From
the deploy repo:

```bash
cd ../noorinalabs-deploy
docker compose -f compose.local.yaml up -d kafka minio neo4j
```

This gives you:

- Kafka KRaft single-broker on `localhost:9092` (no ZooKeeper container)
- MinIO on `localhost:9000` (console `localhost:9001`, default creds
  `minio` / `minio12345`)
- Neo4j 5.x on `bolt://localhost:7687` (creds `neo4j` / `changeme` for
  local dev)

### Create the MinIO bucket

The workers expect `noorinalabs-pipeline` to exist. First run only:

```bash
docker run --rm --network host \
  -e MC_HOST_local=http://minio:minio12345@localhost:9000 \
  minio/mc mb local/noorinalabs-pipeline
```

Or via the MinIO console at `http://localhost:9001`.

### Create Kafka topics

KRaft creates topics on first publish, but explicit creation pins
partition count and avoids the "topic exists with default 1 partition,
can never re-partition" surprise:

```bash
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic pipeline.raw.landed --partitions 3 --replication-factor 1
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic pipeline.dedup.done --partitions 3 --replication-factor 1
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic pipeline.enrich.done --partitions 3 --replication-factor 1
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic pipeline.normalize.done --partitions 3 --replication-factor 1
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic pipeline.dlq --partitions 1 --replication-factor 1
```

Verify:

```bash
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Bring up workers

From this repo:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export PIPELINE_BUCKET=noorinalabs-pipeline
export S3_ENDPOINT_URL=http://minio:9000
export AWS_ACCESS_KEY_ID=minio
export AWS_SECRET_ACCESS_KEY=minio12345
export NEO4J_URI=bolt://neo4j:7687
export NEO4J_USER=neo4j
export NEO4J_PASSWORD=changeme

docker compose up --build
```

### Smoke check

Tail any worker; you should see `*_worker_starting` then idle. Drop a
test pointer onto the raw topic:

```bash
docker exec -i kafka kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic pipeline.raw.landed <<'EOF'
{"batch_id":"smoke-001","source":"sunnah-api","b2_path":"raw/sunnah-api/2026-01-01/test.parquet","timestamp":"2026-01-01T00:00:00+00:00","record_count":0}
EOF
```

(You will need a corresponding empty Parquet at that key in MinIO; or
expect the batch to land in `pipeline.dlq` with a `NoSuchKey` — that
itself proves the consume → fetch → DLQ path works.)

---

## Worker build and run

### Run a single worker (no Docker)

Useful for fast iteration on the processor logic:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export PIPELINE_BUCKET=noorinalabs-pipeline
export S3_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=minio
export AWS_SECRET_ACCESS_KEY=minio12345

uv run python -m workers.dedup.main      # or enrich / normalize / ingest
```

`ingest-worker` additionally requires `NEO4J_URI`, `NEO4J_USER`,
`NEO4J_PASSWORD`. Its `build_runner()` calls
`driver.verify_connectivity()` at startup so a bad URI / wrong creds
fail fast instead of every message DLQ-ing.

### Build images

```bash
make build-workers
```

Tags as `dedup-worker:dev`, `enrich-worker:dev`,
`normalize-worker:dev`, `ingest-worker:dev`. CI tags as
`ghcr.io/noorinalabs/<worker>:sha-<short>` per the org image-tag
contract (deploy `ghcr-publish.yml`, see references).

### Tests

```bash
make test-workers           # unit tests, no infra (in-memory Kafka + S3 fakes)
make test-integration       # full integration, requires Docker
make check                  # lint + typecheck + unit suite
```

The test suite uses in-memory fakes for Kafka and S3 — `tests/workers/`
runs without any container. Use this for the inner loop.

### Lint and format

```bash
make lint-workers
make format-workers
```

Pre-push **must** include `uvx ruff format --check workers/`. The
`hooks-lint` CI gate blocks on any drift, and per the
`feedback_ruff_format_check_before_push` discipline that lands
additive format-fix commits is to be avoided.

---

## Deploy procedure

> **Status:** Production deployment for the workers is **not yet
> wired**. The deploy compose stack does not currently reference these
> images. Cutover is tracked under `planned_issues 105–108` in
> `ontology/repos/isnad-ingest-platform.yaml` and the multi-repo
> coordination in `noorinalabs-main`.

When the cutover lands, the deploy procedure will follow the
established org pattern (mirror the `noorinalabs-deploy` and
`noorinalabs-isnad-graph` runbooks):

1. **Build + publish** — `ghcr-publish.yml` on push to
   `deployments/phase-3/wave-N` produces `sha-<short>` and
   `stg-<short>` + `stg-latest`. Promote to `prod-<short>` +
   `prod-latest` only via the manual-approval workflow in
   `noorinalabs-deploy` (org image-tag Contract v6, canonical comment
   linked in references).
2. **Pre-deploy** — `noorinalabs-deploy/scripts/db-migrate-gate.sh`
   already handles user-service / isnad-graph; the ingest-worker
   bringup needs an analogous "Neo4j connectivity probe" gate before
   promotion. **Carry-forward.**
3. **Stg cutover** — auto-deploy on push to wave branch, smoke via
   `verify-deploy` workflow. Acceptance: workers show
   `*_worker_starting` in logs, no DLQ traffic for 5 min.
4. **Prod cutover** — manual approval on the `promote-prod` workflow.
   Same acceptance battery as stg.
5. **Post-cutover** — confirm one real batch flows raw → staged via
   topic offsets and Neo4j MERGE counts (see triage commands below).

The compose service definitions in
`docker-compose.yaml` are the authoritative starting point — they
already declare the env contract. The deploy compose just needs to
import them with the right `external` networks once Kafka and Neo4j
are on the prod stack.

---

## Rollback

Same shape as the org rollback runbook in `noorinalabs-deploy`:

### Image rollback (most common)

A bad worker image after a deploy:

```bash
# In noorinalabs-deploy on the prod box:
docker compose pull dedup-worker:prod-<previous-short>
docker compose up -d dedup-worker
```

Rolling back one worker is safe — Kafka offsets are committed only on
successful processing, so the previous version picks up exactly where
the bad version stopped (or from the last good offset before the bad
version started DLQ-ing).

### Topic rollback (rare, surgical)

If a bad batch poisoned a downstream topic, the operator can:

1. Pause the affected consumer group:
   `kafka-consumer-groups.sh --bootstrap-server <broker> --group <worker> --reset-offsets --to-earliest --topic <topic> --execute`
   (use `--dry-run` first).
2. Replay from a known-good offset rather than `--to-earliest` if only
   a slice is bad:
   `--reset-offsets --to-offset <N>`.
3. Validate the next 50 messages by tailing the worker log before
   un-pausing.

### Full pipeline reset

`ontology/repos/isnad-ingest-platform.yaml` defines three reset levels:
**stage** (one stage prefix + downstream topics), **source** (all
stages for one source), **full** (all B2 + all topics + Neo4j hadith
data + PG hadith metadata; preserves user/auth/RBAC). The
`obliterate` machinery itself is **carry-forward** under planned issue
#108 — until that lands, treat full reset as a manual operator
procedure to be designed during the cutover (do not improvise).

---

## Common-failure triage

### Symptom: worker fails to start

Check the startup log line. Workers log
`<name>_worker_starting` immediately after `configure_logging()` and
before `build_runner()`.

| Log line missing? | Cause | Fix |
|---|---|---|
| Process exits with `KeyError: 'KAFKA_BOOTSTRAP_SERVERS'` | Required env var unset | See [Worker build and run](#worker-build-and-run) for the env list |
| Process exits with `kafka.errors.NoBrokersAvailable` | Kafka unreachable | See [Kafka topic / broker](#kafka-topic--broker-state) |
| `ingest-worker` exits with `neo4j.exceptions.ServiceUnavailable` from `verify_connectivity()` | Neo4j unreachable | Check `NEO4J_URI`, `bolt://` scheme, prod creds |

### Symptom: messages pile up on a topic

Consumer-group lag rising with no progress.

```bash
# Lag per partition
kafka-consumer-groups.sh --bootstrap-server <broker> --describe --group <worker-name>
```

Diagnostic ladder:

1. Is the worker container running? `docker ps | grep <worker>`.
2. Is it stuck in a hot loop? Tail logs — repeated `idempotent_skip`
   means upstream is replaying a `batch_id` already processed; not a
   bug, but worth investigating producer behavior.
3. Are messages going to DLQ instead? `kafka-consumer-groups.sh
   --describe --group <whoever-tails-dlq>` or just consume:
   `kafka-console-consumer.sh --topic pipeline.dlq --from-beginning`.
4. Is the upstream stage producing? Check the previous worker's lag —
   often the "stuck" stage is downstream of a real upstream failure.

### Symptom: every message DLQs

Look at the DLQ records — they include `error_class`, `error_message`,
and `error_traceback`:

```bash
kafka-console-consumer.sh --bootstrap-server <broker> \
  --topic pipeline.dlq --from-beginning --max-messages 5 | jq .
```

Common patterns:

| `error_class` | Likely cause |
|---|---|
| `NoSuchKey` (boto3 ClientError) | Pointer references a B2 path that doesn't exist — producer / acquire stage bug, or the B2 prefix was wiped between produce and consume |
| `botocore.exceptions.ClientError` 403 | Wrong creds or wrong endpoint URL (B2 vs MinIO mismatch — `S3_ENDPOINT_URL` set in prod by accident, or unset locally) |
| `pydantic.ValidationError` | Upstream producer emitted a malformed pointer; check `workers/lib/message.py` schema and producer source |
| `neo4j.exceptions.ConstraintError` | Ingest stage MERGE conflict — usually a node-label outside `ALLOWED_NODE_LABELS` (see `workers/lib/topics.py`) |

### Kafka topic / broker state

```bash
# Broker liveness
kafka-broker-api-versions.sh --bootstrap-server <broker>

# All topics + partitions
kafka-topics.sh --bootstrap-server <broker> --describe

# A specific topic
kafka-topics.sh --bootstrap-server <broker> --describe --topic pipeline.dedup.done
```

If a topic is missing, recreate it (see local-dev setup) — KRaft will
auto-create on first publish but with default 1 partition, which we
do not want in prod.

### KRaft quorum

KRaft replaces ZooKeeper with an internal Raft quorum on the brokers.
For our single-broker setup the quorum is trivial; for multi-broker
prod (when we get there):

```bash
# Quorum status
kafka-metadata-quorum.sh --bootstrap-server <broker> describe --status

# Replication
kafka-metadata-quorum.sh --bootstrap-server <broker> describe --replication
```

Lost quorum (majority of controllers down) means the cluster is
unwritable until quorum is restored. Don't touch `meta.properties` or
the `__cluster_metadata` topic without an explicit recovery plan
documented in `noorinalabs-deploy` — escalate.

### MinIO bucket access

```bash
# List buckets via mc
mc ls local/

# List the pipeline bucket prefix
mc ls --recursive local/noorinalabs-pipeline/raw/

# Permissions
mc admin policy info local readwrite
```

If MinIO is responsive but `403`s:

- Wrong access key / secret in worker env.
- `S3_ENDPOINT_URL` pointing at the wrong host (MinIO is HTTP not
  HTTPS in local dev).
- Bucket policy missing — should be unrestricted for the dev creds.

---

## On-call escalation

This repo is **planned / scaffold** as of P3W6. Until production
cutover lands, on-call paging for it is **N/A** — there is no prod
traffic. After cutover:

1. **First responder:** the on-call rotation in `noorinalabs-deploy`
   (cluster-level Kafka, MinIO, Neo4j ownership lives there). The
   on-call uses the dashboard at `noorinalabs-deploy`'s observability
   stack — see `reference_ssh_topology` and the deploy
   blackbox/Prometheus configuration.
2. **Code owner escalation:** for worker-internal bugs (processor
   logic, message schema, DLQ patterns), escalate to the active
   ingest-platform implementer per the wave roster in
   `.claude/team/roster/` and the meta-issue
   `noorinalabs-main#284`.
3. **Severity:**
   - **SEV-3** (default) — DLQ traffic non-zero but processed batches
     still flowing; dedup/enrich/normalize backlogs growing slowly.
     Triage during business hours.
   - **SEV-2** — one stage fully wedged (no committed offsets in 15
     min) but data is safe in the previous prefix. Page implementer.
   - **SEV-1** — KRaft quorum lost / Neo4j unreachable for >10 min /
     B2 outage. Escalate to `noorinalabs-deploy` on-call; this repo's
     workers are downstream victims.

The DLQ is **not** a paging signal by itself — it is an expected
feature for poison-pill isolation. Page only when DLQ rate exceeds
some-rolling-baseline (alert thresholds **carry-forward**, see issues
below).

---

## References

Internal:

- `ontology/repos/isnad-ingest-platform.yaml` — single source of truth
  for topology, prefixes, reset levels.
- `ontology/services.yaml` § `isnad-ingest-platform` — service-level
  context.
- `workers/README.md` — worker-layout reference (this runbook is the
  operational layer above it).
- `workers/lib/topics.py` — canonical topic constants and node-label
  vocabulary.
- `workers/lib/runner.py` — consumer loop, idempotency, DLQ routing.
- `workers/lib/message.py` — pointer-message Pydantic schema.
- `noorinalabs-data-acquisition/src/.../kafka_producer.py` — upstream
  producer that imports `pipeline.raw.landed` from this repo's
  `workers.lib.topics`.
- `noorinalabs-deploy` — runtime cluster (Kafka, MinIO, Neo4j),
  on-call rotation, observability, image-tag contract.

Operational:

- `noorinalabs-main#284` — P3W6 meta-issue (this wave).
- Org image-tag Contract v6 (canonical) — `sha-<short>` +
  `stg-<short>` + `stg-latest` (ghcr-publish.yml) + `prod-<short>` +
  `prod-latest` (deploy#84 only). See memory entry
  `project_w10_image_tag_contract`.

External:

- Apache Kafka KRaft mode docs.
- MinIO server quickstart.
- Backblaze B2 S3-compatible API.

---

## Known gaps and carry-forwards

The following operational concerns are **explicitly out of scope** for
this scaffold runbook and are tracked separately:

- **#105** — B2 bucket bootstrap with stage-prefix hierarchy (Terraform).
- **#106** — Production Kafka KRaft topology in `noorinalabs-deploy`
  with the five pipeline topics pre-created.
- **#107** — Worker-container deploy wiring in the prod compose.
- **#108** — `obliterate` / reset machinery (stage / source / full).
- Durable idempotency checkpoint store
  (`workers/lib/runner.py::InMemoryCheckpoint` is dev-only — TODO in
  source).
- DLQ paging thresholds and Prometheus alert rules.
- Neo4j connectivity probe in the deploy db-migrate-gate analog.

Where this runbook says "carry-forward", an issue exists or will be
filed against this repo or `noorinalabs-deploy`. Do not extend this
document with operational guidance for unbuilt machinery — update it
as those items land.
