# normalize-worker image.
# Build from repo root: docker build -f workers/normalize/Dockerfile -t normalize-worker .
#
# Local-dev image only: the DEPLOYED stack pulls the top-level multi-worker
# Dockerfile and selects the stage via the compose `command:` (ip#83, and
# noorinalabs-deploy compose/docker-compose.prod.yml). This file exists for the
# self-contained local compose (docker-compose.yaml).
#
# Kept structurally in lockstep with the top-level Dockerfile (ip#132): same
# base/builder/runtime split, same digest, no build toolchain and no curl in the
# runtime layer. Nothing here needs a compiler — every non-dev dependency ships a
# pure-python or prebuilt manylinux wheel — and no consumer shells curl (neither
# the prod compose nor this repo's own docker-compose.yaml defines a healthcheck
# against these workers).

# Digest-pinned base + in-image apt upgrade (charter § Base Image Pinning,
# noorinalabs-main#735 / #744). Digest kept in lockstep with the top-level
# Dockerfile and the sibling repos' python:3.14-slim pin.
#
# This image is NOT published. ghcr-publish.yml builds only the top-level
# `Dockerfile` (`file: Dockerfile`) and never touches this one, so the
# `no-cache-filters: base` exclusion that forces the apt upgrade to re-run on
# every publish does NOT apply here — a local `docker build` of this file will
# happily serve the apt layer from the local cache. The stage is named `base`
# purely to keep the five Dockerfiles one shape (ip#127); do not read the name
# as evidence that anything un-caches it.
FROM python:3.14-slim@sha256:44dd04494ee8f3b538294360e7c4b3acb87c8268e4d0a4828a6500b1eff50061 AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    INGEST_CHECKPOINT_BACKEND=pg

RUN apt-get update \
 && apt-get -y upgrade \
 && rm -rf /var/lib/apt/lists/*

FROM base AS builder

ENV UV_LINK_MODE=copy

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv venv /app/.venv \
 && uv export --frozen --no-dev --no-emit-project --format requirements-txt -o /tmp/requirements.txt \
 && uv pip install --python /app/.venv/bin/python --require-hashes -r /tmp/requirements.txt \
 && rm /tmp/requirements.txt

FROM base AS runtime

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY src ./src
COPY workers ./workers

# Fail the build, not the container, if this stage stops importing.
RUN python -c "import workers.normalize.main"

RUN useradd --uid 1000 --create-home --shell /bin/bash worker \
 && chown -R worker:worker /app
USER worker

CMD ["python", "-m", "workers.normalize.main"]
