# metadata/

Lightweight custom metadata layer — not a full data catalog, but enough to
answer the questions a data platform team actually gets asked: pipeline run
history, table freshness, schema version history (drift detection),
ownership, and lineage.

- `schema.py` — SQLAlchemy Core `Table` definitions for the five tables
  (`pipeline_runs`, `table_freshness`, `schema_versions`, `table_ownership`,
  `lineage`), mirroring
  `docker/postgres/init-warehouse/02_schema_metadata.sql` (kept manually in
  sync — no Alembic yet, a natural next step once this schema needs to
  evolve under active use). Deliberately dialect-portable: no Postgres-only
  types, and `to_connectable(engine)` remaps the `metadata` schema away for
  any non-Postgres engine via SQLAlchemy's `schema_translate_map` — this is
  what lets `client.py` be tested against a real in-memory SQLite engine
  (genuine SQL execution, not mocks) with zero live Postgres required.
- `client.py` — the tracking API. `start_run`/`complete_run` record a
  pipeline run's lifecycle directly; `track_run(engine, pipeline_name,
  layer, source_name, table_name, batch_date)` is the usual entrypoint — a
  context manager that starts a run, yields a handle the caller sets
  `row_count` (and optionally `columns`) on, then records success/failure
  automatically (a failure never marks the table "fresh"). `record_schema_
  version` is the actual schema-drift detector: it fingerprints a table's
  sorted column list and only writes a new version row when that
  fingerprint changes. `get_freshness`/`get_lineage`/`get_ownership` read
  back what's been recorded.

## Wired in as opt-in

`pipelines/ingestion/base.py`'s `BaseIngestion` and `pipelines/gold/runner.py`'s
`run_gold_build` both accept an optional `metadata_engine` parameter
(default `None` = tracking skipped entirely). This is deliberate: ingestion
and Gold-build code needs to stay fully usable and testable without a
metadata database — every existing test in `tests/ingestion/` and
`tests/gold/` continues to pass unmodified with no engine supplied. Airflow
tasks pass a real engine (`metadata.client.default_engine()`, built from
`settings.warehouse_db_url`) explicitly. Bronze/Silver/quality wiring
follows the identical pattern — `metadata_engine=None` by default, opt in by
passing an engine — as a natural extension, not yet done for every module.

## Verified locally

7 tests in `tests/metadata/test_client.py` running real SQL against an
in-memory SQLite engine: run lifecycle (start/complete), `track_run`
success and failure paths (confirming a failed run does *not* update
freshness), schema-drift detection (including a same-columns-different-order
case correctly *not* counting as drift), and lineage/ownership lookups.
Plus 2 new tests each in `tests/ingestion/test_base.py` and
`tests/gold/test_runner.py` proving metadata tracking fires when an engine
is supplied and is silently skipped when it isn't.
