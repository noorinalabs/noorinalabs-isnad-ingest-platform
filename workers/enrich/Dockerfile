# enrich-worker image.
# Build from repo root: docker build -f workers/enrich/Dockerfile -t enrich-worker .

FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_SYSTEM_PYTHON=1 \
    UV_LINK_MODE=copy

RUN apt-get update \
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
