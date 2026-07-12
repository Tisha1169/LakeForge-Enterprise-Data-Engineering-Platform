# tests/

pytest suite, mirroring the structure of `pipelines/` so every module has an
obvious corresponding test module.

- `ingestion/`, `bronze/`, `silver/`, `gold/`, `metadata/` — unit and
  integration tests for the matching `pipelines/` (and `spark/`, `dbt/` where
  relevant) modules.

Target: >80% coverage, enforced in CI (Phase 19). Fixtures use `sample_data/`
rather than hitting real source systems.
