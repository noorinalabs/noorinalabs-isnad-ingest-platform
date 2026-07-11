# Project Memory — noorinalabs-isnad-ingest-platform

This is the always-loaded index (one line per memory). Topic files in this directory are read on demand when a line looks relevant.

- [Data pipeline arch](project_data_pipeline_architecture.md) — B2 storage, Kafka/KRaft pipeline, acquisition-vs-platform repo split, MinIO local dev.
- [main#136 E2E lands in ingest](project_main136_pipeline_e2e_lands_in_ingest.md) — main#136 pipeline-worker E2E scenarios land in ingest-platform (not deploy/not main), ingest persona.
- [ecdsa Minerva standing item](project_ecdsa_minerva_standing_item.md) — PYSEC-2026-1325 pip-audit-ignored: RS256-only allowlist makes the ECDSA path unreachable; switching to ES* REVOKES it. pip-audit --ignore-vuln matches aliases. ingest#128.
