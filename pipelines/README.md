# pipelines/

Core Python business logic for the platform, organized by medallion layer.
Airflow DAGs call into this package; this package has no Airflow dependency
itself, so it is independently unit-testable (see `tests/`).

- `ingestion/` — pulls data from source systems into the MinIO landing zone.
  Subfolders per source type: `api/` (Customer API), `db/` (Sales, Inventory
  Postgres extraction), `files/` (Supplier CSV drop).
- `bronze/` — lands raw ingested data as immutable, partitioned Parquet with
  metadata capture. No transformation.
- `silver/` — thin orchestration around the PySpark jobs in `spark/jobs/`
  (job submission, parameter resolution) — the transformation logic itself
  lives in `spark/`.
- `gold/` — thin orchestration around dbt runs (invoking `dbt run`/`dbt
  test` for the relevant model selection) — the transformation logic itself
  lives in `dbt/`.

Every module here is config-driven (see `config/`) and takes no hardcoded
source paths, connection strings, or credentials.
