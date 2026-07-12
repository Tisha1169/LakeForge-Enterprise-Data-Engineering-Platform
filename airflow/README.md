# airflow/

Orchestration layer. Owns *when* and *in what order* pipelines run — contains
no business/transformation logic itself, only calls into `pipelines/`,
`spark/`, and `dbt/`.

- `dags/` — one DAG file per pipeline (e.g. `ingest_customers.py`,
  `bronze_to_silver_sales.py`, `silver_to_gold.py`). DAGs are thin: they wire
  together tasks and dependencies, delegating actual work to code in
  `pipelines/` and `spark/`.
- `plugins/` — custom Airflow operators/hooks/sensors shared across DAGs
  (e.g. a `MinIOSensor` that waits for a landing-zone file, a
  `GreatExpectationsOperator` wrapper).
- `config/` — Airflow-specific configuration (connection templates, variable
  definitions) kept out of DAG code so credentials/environments can change
  without touching pipeline logic.

Built out in Phase 11.
