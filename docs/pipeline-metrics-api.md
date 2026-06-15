# Pipeline metrics & reprocess API (issue #76)

Admin-only HTTP surface the **isnad-graph admin data panel** fetches to show
pipeline health and to trigger a reprocess. All routes are served by the
ingest-platform FastAPI app (`src/api/app.py`), mounted under `/admin`, and
guarded by `require_admin` — the same user-service RS256 admin JWT that
authorizes the isnad-graph admin dashboard and the reset endpoints.

- Base path: `/admin`
- Auth: `Authorization: Bearer <user-service access token with role admin>`
  - missing/invalid token → `401`; valid non-admin → `403`; user-service
    JWKS unreachable → `503`

Run locally: `uvicorn src.api.app:create_app --factory --port 8002`.

## Stage vocabulary

The pipeline runs four consuming stages, in order. The `stage` names below
are the stable identifiers used by both the metrics and reprocess routes.

| stage | consumer group | consumes topic | produces topic |
| --- | --- | --- | --- |
| `dedup` | `dedup-worker` | `pipeline.raw.landed` | `pipeline.dedup.done` |
| `enrich` | `enrich-worker` | `pipeline.dedup.done` | `pipeline.enrich.done` |
| `normalize` | `normalize-worker` | `pipeline.enrich.done` | `pipeline.normalize.done` |
| `graph-load` | `ingest-worker` | `pipeline.normalize.done` | (terminal) |

This table is the source of truth in `src/pipeline/stages.py` and is pinned
to the worker wiring by `tests/test_pipeline/test_stages.py`.

## GET /admin/metrics/lag

Per-stage Kafka consumer lag — how far each stage's consumer group is behind
the head of the topic it consumes. `lag = end_offset - committed_offset`,
clamped at 0; a partition the group has never committed counts its whole
head offset as backlog. `total_lag` (per stage and overall) is the sum over
partitions/stages — the headline "is the pipeline keeping up?" number.

Response `200`:

```json
{
  "stages": [
    {
      "stage": "dedup",
      "consumer_group": "dedup-worker",
      "topic": "pipeline.raw.landed",
      "total_lag": 12,
      "partitions": [
        { "partition": 0, "end_offset": 100, "committed_offset": 88, "lag": 12 }
      ]
    }
  ],
  "total_lag": 12
}
```

- `committed_offset` is `null` when the group has never committed that
  partition (the partition's `lag` then equals `end_offset`).
- `stages` always lists all four stages in pipeline order; a stage whose
  topic has no partitions yet reports `total_lag: 0` and `partitions: []`.

## GET /admin/metrics/storage

B2 object-store size, broken down by the pipeline's stage prefixes plus a
bucket-wide rollup. `total_bytes` is the sum of object sizes under each
prefix; `object_count` is the number of objects.

Response `200`:

```json
{
  "prefixes": [
    { "prefix": "raw/", "object_count": 1432, "total_bytes": 9105533 },
    { "prefix": "dedup/", "object_count": 1280, "total_bytes": 8401200 },
    { "prefix": "enriched/", "object_count": 1280, "total_bytes": 9550120 },
    { "prefix": "normalized/", "object_count": 640, "total_bytes": 4120044 },
    { "prefix": "staged/", "object_count": 640, "total_bytes": 4010990 }
  ],
  "total_object_count": 5272,
  "total_bytes": 35187887
}
```

The prefixes match the reset surface's B2 stage layout (`raw/`, `dedup/`,
`enriched/`, `normalized/`, `staged/`).

## POST /admin/reprocess

Re-run the pipeline from a chosen stage **without deleting staged B2 data**
(that is the reset surface, `POST /admin/reset/*`). Reprocessing from stage
*X* rewinds *X*'s consumer group to the start of its consume topic and clears
the idempotency checkpoints for *X* and every downstream stage, so the
re-delivered batches are processed again instead of being skipped as
already-seen. Only the entry stage's group is rewound — downstream stages
re-consume naturally as *X* re-emits.

Request:

```json
{ "stage": "enrich", "dry_run": false }
```

- `stage` (required) — one of the stage names above. Unknown → `422`.
- `dry_run` (default `false`) — resolve the plan and write a dry-run audit
  entry **without** touching Kafka or Postgres. The admin panel should call
  this first to preview the affected stages.

Response `200`:

```json
{
  "from_stage": "enrich",
  "dry_run": false,
  "audit_entry_path": "data/audit/2026-06-14T….json",
  "summary": {
    "from_stage": "enrich",
    "rewound_group": "enrich-worker",
    "rewound_topic": "pipeline.dedup.done",
    "affected_stages": ["enrich", "normalize", "graph-load"],
    "checkpoints_cleared": { "enrich": 3, "normalize": 5, "graph-load": 2 },
    "dry_run": false
  }
}
```

Every call (dry-run included) writes an audit entry under `data/audit/`,
matching the reset surface, so a reprocess is attributable in the audit log.

**Operational note.** Rewinding a consumer group's committed offsets takes
effect for the worker fleet on its next rebalance/restart — Kafka only allows
an offset to be altered for a group with no active members on that partition.
This endpoint triggers the rewind and checkpoint clear; the running workers
pick it up. This is the same operational contract the reset endpoints carry.
