.PHONY: help setup setup-hooks acquire parse resolve load enrich pipeline test test-integration test-workers lint lint-workers format format-workers typecheck check clean validate validate-staging profile-data build-workers structural-ontology structural-ontology-check

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Install dependencies with uv (includes ML group for dedup)
	uv sync --group ml

setup-hooks: ## Configure pre-commit hooks (pre-commit + commit-msg + pre-push) + merge driver
	uv run pre-commit install --hook-type pre-commit --hook-type commit-msg --hook-type pre-push
	@# Register the ontology-codegraph union merge-driver (#116). The driver
	@# lives in the sibling noorinalabs-main; if it can't be located this is a
	@# soft warning (git falls back to a text merge for code-graph.json).
	@python3 scripts/structural_ontology.py register-merge-driver || \
		echo "warn: ontology-codegraph merge driver not registered (noorinalabs-main generator not found); see .gitattributes"
	@echo "Git hooks configured (pre-commit/commit-msg/pre-push via pre-commit framework + merge driver)."

acquire: ## Phase 1: Download all data sources
	uv run isnad-ingest acquire

parse: ## Phase 1: Parse raw data into staging Parquet files
	uv run isnad-ingest parse

resolve: ## Phase 2: Entity resolution (NER + disambiguation + dedup)
	uv run isnad-ingest resolve

load: ## Phase 3: Load graph into Neo4j
	uv run isnad-ingest load

enrich: ## Phase 4: Compute metrics, topics, historical overlay
	uv run isnad-ingest enrich

pipeline: ## Run full pipeline end-to-end
	$(MAKE) acquire
	$(MAKE) parse
	$(MAKE) resolve
	$(MAKE) load
	$(MAKE) enrich

test: ## Run pytest suite
	uv run pytest

test-integration: ## Run integration tests (requires Docker)
	uv run pytest tests/integration/ -v -m integration

test-workers: ## Run streaming-worker unit tests (no Docker/Kafka required)
	uv run pytest tests/workers/ -v --confcutdir=tests/workers

lint: ## Run ruff linter
	uv run ruff check src/ workers/ tests/

lint-workers: ## Ruff on workers package only
	uv run ruff check workers/ tests/workers/

format: ## Run ruff formatter
	uv run ruff format src/ workers/ tests/

format-workers: ## Ruff format on workers package only
	uv run ruff format workers/ tests/workers/

build-workers: ## Build all 4 worker Docker images
	docker build -f workers/dedup/Dockerfile     -t dedup-worker:dev     .
	docker build -f workers/enrich/Dockerfile    -t enrich-worker:dev    .
	docker build -f workers/normalize/Dockerfile -t normalize-worker:dev .
	docker build -f workers/ingest/Dockerfile    -t ingest-worker:dev    .

typecheck: ## Run mypy type checker
	uv run mypy src/

check: ## Run all CI checks (lint + typecheck + test)
	uv run ruff check src/ tests/
	uv run ruff format --check src/ tests/
	uv run mypy src/
	uv run pytest tests/ -v --tb=short -x -m "not integration"
	@echo "All checks passed."

clean: ## Remove staging data and caches
	rm -rf data/staging/*
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	find . -type d -name .mypy_cache -exec rm -rf {} +
	find . -type d -name .ruff_cache -exec rm -rf {} +

validate: ## Run data quality validation (strict mode, JSON report)
	uv run isnad-ingest validate --strict --output-json data/reports/validation_report.json

validate-staging: ## Validate staging Parquet files (warn mode)
	uv run isnad-ingest validate

profile-data: ## Profile staging Parquet files
	uv run python scripts/data_profile.py

structural-ontology: ## Regenerate the committed structural ontology index (ontology/structural/)
	python3 scripts/structural_ontology.py emit

structural-ontology-check: ## Fail if the committed structural ontology index is stale vs source
	python3 scripts/structural_ontology.py check
