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

# Digest-pinned base + in-image apt upgrade (charter § Base Image Pinning,
# noorinalabs-main#735 / #744). The @sha256 digest freezes the starting layer
# against floating-tag drift; the `apt-get -y upgrade` below closes the second
# failure mode (within-tag package drift — the shape isnad-graph#853 hit). The
# digest is kept in lockstep with the sibling repos' python:3.14-slim pin.
FROM python:3.14-slim@sha256:44dd04494ee8f3b538294360e7c4b3acb87c8268e4d0a4828a6500b1eff50061

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

# No default stage — compose overrides `command:` per service. A bare run
# without a stage is an operator error, so fail fast with the valid choices.
CMD ["python", "-c", "import sys; sys.exit('select a pipeline stage, e.g. python -m workers.dedup.main (one of: dedup, enrich, normalize, ingest)')"]
