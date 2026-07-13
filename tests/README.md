# tests/

pytest suite, mirroring the structure of `pipelines/` so every module has an
obvious corresponding test module.

- `ingestion/`, `bronze/`, `silver/`, `gold/`, `metadata/`, `monitoring/`,
  `data_quality/` — unit and integration tests for the matching `pipelines/`
  (and `spark/`, `data_quality/`, `metadata/`) modules.
- `airflow/test_dags.py` — real `DagBag`-based structural tests (import
  errors, task graph shape, retry configuration) for `airflow/dags/`. Skips
  cleanly via `pytest.importorskip("airflow.models")` when Airflow isn't
  installed — deliberately *not* `importorskip("airflow")`: this project's
  own `airflow/` directory (no `__init__.py`) becomes a namespace package
  for the bare name `airflow` whenever the real `apache-airflow` isn't
  installed, so that check wouldn't actually skip (reproduced directly).

No `dbt/` test directory here — the dbt project has its own test story
(`dbt test`, `dbt snapshot` twice against local fixtures) documented in
`dbt/README.md`, run via `dbt` itself rather than pytest.

Every test hits real logic against real backends where one exists locally
with no external service required — a moto-mocked S3 for storage/Bronze/
Silver/Gold I/O, a real local Spark session for PySpark transforms, an
in-memory SQLite engine (via SQLAlchemy's `schema_translate_map`) for the
metadata layer, and a real Great Expectations checkpoint for data quality —
not hand-rolled mocks of our own code. Fixtures use `sample_data/` rather
than hitting real source systems.

**Coverage: 91%** (`pipelines`, `metadata`, `monitoring`, `config`,
`data_quality`, `spark`; measured via `pytest --cov`), above the >80%
target. Remaining gaps are almost entirely each Spark Silver job's
`run()`/`__main__` entrypoint (thin wrappers around `clean()`, which is
100% covered per job) — not chased further past that point since the actual
logic is already fully exercised.
