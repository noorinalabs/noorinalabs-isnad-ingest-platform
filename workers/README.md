# Streaming Pipeline Workers

Kafka-driven streaming workers for the ingestion pipeline. Each worker
consumes a topic, reads a batch from S3-compatible storage (B2 or
MinIO), processes it, writes results back to storage, and publishes a
pointer on the next-stage topic.

## Topology

| Worker | Consumes | Produces | Port from |
|---|---|---|---|
| `dedup-worker` | `pipeline.raw.new` | `pipeline.dedup.done` | `src/resolve/` |
| `enrich-worker` | `pipeline.dedup.done` | `pipeline.enrich.done` | `src/enrich/` |
| `normalize-worker` | `pipeline.enrich.done` | `pipeline.norm.done` | (new) |
| `ingest-worker` | `pipeline.norm.done` | Neo4j (terminal) | `src/graph/` |

All failures route to `pipeline.dlq`.

## Message contract (pointer-based)

```json
{
  "batch_id": "uuid",
  "source": "sunnah-api",
  "b2_path": "raw/sunnah-api/2026-04-13/hadiths.parquet",
  "timestamp": "2026-04-13T12:00:00+00:00",
  "record_count": 1234
}
```

See `workers/lib/message.py` for the Pydantic schema.

## Layout

```
workers/
  lib/                     # shared consumer loop, message schema, S3 client, DLQ, metrics
  dedup/
    main.py                # entry point
    processor.py           # per-batch logic
    Dockerfile
  enrich/                  # same layout
  normalize/
  ingest/
  README.md
```

## Environment

Every worker requires:

- `KAFKA_BOOTSTRAP_SERVERS` — broker address
- `PIPELINE_BUCKET` — S3 bucket name (default `noorinalabs-pipeline`)
- `S3_ENDPOINT_URL` — set for MinIO/local; unset for B2 prod
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`

`ingest-worker` additionally requires `NEO4J_URI`, `NEO4J_USER`,
`NEO4J_PASSWORD`.

## Running locally

```bash
docker compose up --build
```

Assumes Kafka and MinIO are on the same Docker network (brought up by
`noorinalabs-deploy`). See `docker-compose.yaml` for the full wiring.

## Testing

```bash
uv run pytest tests/workers/
```

Tests use in-memory fakes for Kafka and S3 — no infra required.

## Scope notes (Wave 9 scaffold)

This is the happy-path scaffold for issue #107. The processors are
deliberately pass-through — they wire up the full topology (consume /
fetch / write / publish / DLQ) but defer the real dedup / enrichment /
normalization / Neo4j load logic to follow-up issues. Look for `TODO`
comments in each `processor.py` for the exact ports pending.
