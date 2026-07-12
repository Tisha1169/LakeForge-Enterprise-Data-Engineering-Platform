# Developer Guide

## Local setup

OpenLake uses [`uv`](https://docs.astral.sh/uv/) for Python dependency
management (Python 3.13).

```bash
# 1. Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Install project dependencies (all optional groups: spark, airflow, dbt, quality, dev)
make install
# equivalent to: uv sync --all-extras

# 3. Copy the environment template and fill in local values
cp .env.example .env

# 4. Run linting / type checking / tests
make lint
make typecheck
make test
```

Full Docker-based platform startup (`docker compose up`) is documented
starting in Phase 4.

## Adding a new data source

1. Add a YAML file describing the source under `config/sources/`.
2. Add an ingestion module under `pipelines/ingestion/{api,db,files}/`.
3. Add a corresponding test under `tests/ingestion/`.
4. Wire it into an Airflow DAG under `airflow/dags/`.

No source-specific values should ever be hardcoded in Python — they belong in
the YAML config or `.env`.

(This guide expands with deployment and pipeline-authoring detail in Phase 18.)
