# enrich-worker image.
# Build from repo root: docker build -f workers/enrich/Dockerfile -t enrich-worker .

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
    UV_SYSTEM_PYTHON=1 \
    UV_LINK_MODE=copy \
    INGEST_CHECKPOINT_BACKEND=pg

RUN apt-get update \
 && apt-get -y upgrade \
 && apt-get install -y --no-install-recommends build-essential curl \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv export --frozen --no-dev --no-emit-project --format requirements-txt -o /tmp/requirements.txt \
 && uv pip install --system --require-hashes -r /tmp/requirements.txt \
 && rm /tmp/requirements.txt

COPY src ./src
COPY workers ./workers

RUN useradd --uid 1000 --create-home --shell /bin/bash worker \
 && chown -R worker:worker /app
USER worker

CMD ["python", "-m", "workers.enrich.main"]
