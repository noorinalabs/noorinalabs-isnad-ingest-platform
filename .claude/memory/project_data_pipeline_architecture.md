---
name: project_data_pipeline_architecture
description: B2 storage hierarchy, Kafka pipeline, repo split (acquisition vs platform), MinIO local dev, KRaft, admin dashboard data controls
type: project
originSessionId: b26a6371-9ec6-4028-b73a-5fcb52cfb43d
promotion_target: none
status: active
---
## Architecture — decided 2026-04-13

**Storage:** Backblaze B2 single bucket (`noorinalabs-pipeline/`) with prefix-based stages:
- `raw/{source}/{date}/` → `dedup/` → `enriched/` → `normalized/` → `staged/`
- MinIO for local dev (S3-compatible, same code path as B2)

**Message broker:** Kafka with KRaft mode (no ZooKeeper). Steven has prior KRaft experience.

**Repo split:**
- `isnad-graph-ingestion` = **data acquisition** — scrapers, API connectors, downloaders. Output: files in B2 `raw/`. May rename to reflect this narrower scope.
- `isnad-ingest-platform` = **processing pipeline** — Kafka workers + Docker containers. Stages: dedup → enrich → normalize → graph-ingest. Each worker is its own container. Existing processing code (`parse/`, `resolve/`, `enrich/`, `graph/`) migrates here from ingestion repo.

**Reset/obliterate:** Three levels (stage, source, full). Full reset clears all B2 data + Kafka topics + Neo4j hadith data + PG hadith metadata. Preserves user/auth/RBAC data.

**Admin dashboard:** 
- Non-admin users get 404 (not 403 or redirect) — hides admin existence
- Sections: user management (rewire 501 stubs to user-service), data management (B2/Kafka/Neo4j controls), monitoring links (Grafana)
- Only `parametrization` (Steven's OAuth identity) gets admin role

**Why:** Steven wants to experiment with the pipeline iteratively — scrape locally, process via Kafka, obliterate and restart. The separation lets acquisition run locally while the pipeline runs in Docker.
