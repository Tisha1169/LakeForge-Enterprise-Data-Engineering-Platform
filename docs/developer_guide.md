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

`pytest`'s default `addopts` (`pyproject.toml`) run coverage and enforce the
80% floor (`--cov-fail-under=80`) automatically — a plain `pytest` run fails
if coverage regresses, no separate CI-only check needed.

### Extra local requirements per layer

- **PySpark tests need a JVM.** `brew install openjdk@17` (macOS), then
  `export JAVA_HOME=$(brew --prefix openjdk@17)` before running
  `pytest tests/silver/`. See `spark/README.md`.
- **dbt tests need `dbt-core`+`dbt-duckdb`** (installed via `make install`'s
  `dbt` extra) and local fixtures — see `dbt/README.md`'s "Local dev"
  section.
- **Airflow DAG tests** (`tests/airflow/`) skip automatically if
  `apache-airflow` isn't installed. Airflow 2.x (what
  `docker/airflow/Dockerfile` pins) has no Python 3.13 build as of this
  writing, so installing it locally means accepting a newer major version
  for local validation only — see `airflow/README.md`.

## Full platform startup

```bash
docker compose up -d
```

Brings up Postgres ×3, MinIO, a local Spark cluster, the mock Customer API,
and Airflow (LocalExecutor). See [docker/README.md](../docker/README.md)
for the full service list, ports, and what each init script provisions.

## Running the pipeline end-to-end locally

1. `docker compose up -d` — seeds Sales/Inventory Postgres data and starts
   everything.
2. In the Airflow UI (`localhost:8080`), unpause and trigger
   `openlake_bronze_ingestion` for a given logical date.
3. Once it succeeds, `openlake_silver_transform` picks it up automatically
   (`ExternalTaskSensor`), then `openlake_gold_build` after that.
4. `openlake_health_check` runs independently, `@daily` — check its logs
   for a freshness/failure summary across every expected table.
5. Inspect results: MinIO console (`localhost:9001`) for raw Parquet, or
   query the Gold tables directly with DuckDB:
   ```python
   import duckdb
   con = duckdb.connect()
   con.sql("SELECT * FROM read_parquet('s3://openlake-gold/gold/fact_sales/part-0.parquet')")
   ```
   (needs `httpfs` + MinIO S3 credentials configured in the DuckDB session —
   see `dbt/profiles.yml` for the exact settings this project uses.)

## Adding a new data source

1. Add a YAML file describing the source under `config/sources/` (see
   `config/README.md` — `SourceConfig`'s fields).
2. If it's a new DB table: that's it for ingestion — `DatabaseTableIngestion`
   is fully generic over any `(schema_name, table)`. If it's a genuinely new
   transport (not API/DB/file), add a class under
   `pipelines/ingestion/{api,db,files}/` implementing `BaseIngestion.extract()`.
3. Add a corresponding test under `tests/ingestion/`.
4. Add the source to `airflow/dags/bronze_ingestion_dag.py`'s loop — it
   iterates `SOURCE_TABLE_NAMES` from `config/sources.py`, so a new YAML
   file picks it up automatically; only add manual wiring if it needs
   special cadence gating (see `should_run_source`).

No source-specific values should ever be hardcoded in Python — they belong
in the YAML config or `.env`.

## Adding a new Silver table

1. Write `spark/jobs/<name>_silver.py`: a `clean(df)` function (the
   testable core) and a `run(batch_date)` entrypoint following the existing
   jobs' pattern — cast every field explicitly (Bronze hands you strings,
   see `spark/jobs/common.py`'s `bronze_to_spark_df`), drop rows missing
   the grain, dedup via `dedup_latest` on the correct business key.
2. Register it in `pipelines/silver/runner.py`'s `_JOBS` dict.
3. Add it to `airflow/dags/silver_transform_dag.py`'s `PARALLEL_SILVER_JOBS`
   list (or wire an explicit dependency if it needs one, like
   `sales_order_lines`'s dependency on `sales_orders`).
4. Write `tests/silver/test_<name>_silver.py` — test `clean()` directly
   against a small in-memory DataFrame, not `run()` (which needs S3/Bronze
   set up); see any existing `test_*_silver.py` for the pattern.
5. Add an expectation suite in `data_quality/suites.py` if the table has
   meaningful invariants to check.

## Adding a new Gold table

Two places, kept in sync (see `docs/architecture.md` for why both exist):

- `pipelines/gold/<name>.py` — a `build_<name>(...)` function using DuckDB
  SQL via `pipelines/gold/common.py`'s `register_rows`/`fetch_as_dicts`
  helpers, wired into `pipelines/gold/runner.py`'s `run_gold_build`.
- `dbt/models/marts/<name>.sql` — the same logic as a dbt model, added to
  `dbt/models/marts/_marts.yml`'s schema tests.

## Code conventions

- Ruff handles both linting and formatting (`make lint`/`make format`) — no
  separate black/isort/flake8 config.
- No comments explaining *what* code does (names should do that) — only
  *why*, when it's genuinely non-obvious (a workaround, an invariant, a
  real bug this specific code avoids). See any file in `pipelines/` for the
  house style.
- Every I/O boundary goes through the layer's tested client
  (`pipelines/storage.py` for S3, `metadata/client.py` for the metadata DB)
  — no module constructs its own boto3/SQLAlchemy connection.
- Config lives in `config/settings.py` (env vars) or `config/sources/*.yaml`
  (per-source declarations) — never hardcoded, never read via bare
  `os.environ` outside `config/settings.py` itself.

## See also

- [Architecture](architecture.md) — design decisions and rationale.
- [Data Dictionary](data_dictionary.md) — Silver/Gold table and column reference.
- [Deployment Guide](deployment_guide.md) — moving from local Docker to a real cloud environment.
- [Interview Notes](interview_notes.md) — talking points, resume bullets, and real bugs found while building this.
