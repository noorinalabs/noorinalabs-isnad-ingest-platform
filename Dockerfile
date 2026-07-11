# Multi-worker image — the single GHCR artifact the staging/prod compose
# stack pulls for all four pipeline stages (ip#83).
#
# It bundles every worker module (workers.{dedup,enrich,normalize,ingest});
# the running stage is selected at start time by the compose service's
# `command:` (see noorinalabs-deploy compose/docker-compose.prod.yml, the
# profile-gated `dedup-worker` / `enrich-worker` / `normalize-worker` /
# `graph-load-worker` services from deploy#440). The default CMD below is a
# guard so a bare `docker run` of this image fails loudly instead of doing
# nothing.
#
# Build from repo root:
#   docker build -t ghcr.io/noorinalabs/noorinalabs-isnad-ingest-platform:dev .
#
# Published by .github/workflows/ghcr-publish.yml. The per-stage
# workers/<stage>/Dockerfile images remain for the self-contained local-dev
# compose (docker-compose.yaml); this top-level Dockerfile is the one the
# deployed stack consumes.
#
# Multi-stage (ip#132): the runtime image ships ONLY the base OS + the resolved
# virtualenv + application source. It carries no compiler and no curl — see the
# `builder` stage for why neither is needed.

# ---------------------------------------------------------------------------
# base — the OS layer that actually ships.
# ---------------------------------------------------------------------------
# Digest-pinned base + in-image apt upgrade (charter § Base Image Pinning,
# noorinalabs-main#735 / #744). The @sha256 digest freezes the starting layer
# against floating-tag drift; the `apt-get -y upgrade` below closes the second
# failure mode (within-tag package drift — the shape isnad-graph#853 hit). The
# digest is kept in lockstep with the sibling repos' python:3.14-slim pin.
#
# The stage name `base` is LOAD-BEARING, not cosmetic: ghcr-publish.yml passes
# `no-cache-filters: base` (PLURAL — that is the action input; the singular
# `no-cache-filter` is the docker CLI flag and is NOT a declared input, so it is
# silently dropped) to docker/build-push-action. That exclusion is what forces
# the `apt-get -y upgrade` below to actually RUN on every publish instead of
# being replayed from the buildx GHA cache as a no-op (ip#127 / main#947).
#
# Renaming or removing this stage silently re-freezes the apt layer, and
# check_dockerfile_base_pin.py will stay green while it happens — that gate
# proves the upgrade LINE exists, not that it RUNS.
#
# READING THE FIRST PUBLISH LOG AFTER THIS CHANGE: the apt layer's CONTENT
# changes here (the toolchain is gone), so it rebuilds on CONTENT grounds
# whether or not the filter works. This publish therefore does NOT independently
# re-prove `no-cache-filters`, and must not be cited as if it did. ip#127 already
# proved it, on a diff that left this layer byte-identical: run 29140637459 —
# `[base 2/9]` DONE 11.1s, ZERO build-phase CACHED lines, Trivy 10 -> 0.
FROM python:3.14-slim@sha256:44dd04494ee8f3b538294360e7c4b3acb87c8268e4d0a4828a6500b1eff50061 AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    INGEST_CHECKPOINT_BACKEND=pg

RUN apt-get update \
 && apt-get -y upgrade \
 && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# builder — resolve dependencies into a self-contained virtualenv.
# ---------------------------------------------------------------------------
# No build toolchain is installed here, and none is needed: every one of the 64
# packages in the non-dev resolution ships either a pure-python wheel or a
# prebuilt manylinux x86_64 wheel (pyarrow, pandas, lxml, rapidfuzz,
# psycopg-binary, cryptography, numpy included), so nothing is compiled from an
# sdist. Verified against uv.lock with this stage's own `uv export` command:
# zero packages resolve sdist-only.
#
# Do NOT read `--require-hashes` as an sdist *guard* — it is not one. uv.lock
# carries hashes for sdists too, so a hashed sdist would pass the hash check, be
# attempted, and then fail at COMPILE time for want of a toolchain. The safety
# property still holds (a new sdist-only dependency breaks the build loudly, it
# does not silently ship), but the mechanism is the missing compiler, not the
# hash check.
FROM base AS builder

ENV UV_LINK_MODE=copy

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv venv /app/.venv \
 && uv export --frozen --no-dev --no-emit-project --format requirements-txt -o /tmp/requirements.txt \
 && uv pip install --python /app/.venv/bin/python --require-hashes -r /tmp/requirements.txt \
 && rm /tmp/requirements.txt

# ---------------------------------------------------------------------------
# runtime — the published image. No compiler, no curl.
# ---------------------------------------------------------------------------
# `build-essential` pulled libc6-dev -> linux-libc-dev (7 of the 10 HIGH CVEs in
# ip#127 — kernel headers, in a container that will never compile a kernel) and
# `curl` pulled libssh2-1t64 (the other 3). Neither ships now. No consumer needs
# curl: none of the four worker services in noorinalabs-deploy
# compose/docker-compose.prod.yml defines a healthcheck, the repo's own
# docker-compose.yaml is likewise healthcheck-free, and curl appears nowhere in
# src/ or workers/.
FROM base AS runtime

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY src ./src
COPY workers ./workers

# Fail the BUILD, not the deployed container, if a stage stops importing. The
# four `command:` entrypoints the compose stack invokes are exactly these
# modules; each is import-safe (env vars are read inside build_runner(), and
# kafka/boto3/faiss/transformers are imported lazily), so importing all four is
# a cheap proof that the runtime image can start every stage with only the venv.
#
# This guard is NOT inert: the processors import pyarrow/structlog at module
# level, so a venv that failed to COPY forward fails the build here.
RUN python -c "import workers.dedup.main, workers.enrich.main, workers.normalize.main, workers.ingest.main"

RUN useradd --uid 1000 --create-home --shell /bin/bash worker \
 && chown -R worker:worker /app
USER worker

# No default stage — compose overrides `command:` per service. A bare run
# without a stage is an operator error, so fail fast with the valid choices.
CMD ["python", "-c", "import sys; sys.exit('select a pipeline stage, e.g. python -m workers.dedup.main (one of: dedup, enrich, normalize, ingest)')"]
