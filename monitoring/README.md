# monitoring/

Structured logging configuration and pipeline health checks.

- `logging_config.py` (Phase 7) — centralized Python `logging` configuration
  (JSON-structured, level-aware: DEBUG/INFO/WARNING/ERROR/CRITICAL) imported
  by every pipeline module via `get_logger(__name__)` rather than each
  module configuring its own logger.
- `health.py` (Phase 16) — `check_table_health(engine, layer, table_name)`
  answers "is this table healthy" as one of four states: `never_run`
  (no successful run ever recorded), `failing` (a failed run within the
  lookback window — takes priority over staleness, since a table that's
  actively failing is more urgent than one that's merely old), `stale`
  (last successful update older than `max_staleness_hours`), or `healthy`.
  `check_platform_health(engine, expected_tables)` runs that check across
  an explicit list of `(layer, table_name)` pairs — deliberately explicit
  rather than "every table metadata has ever seen," so a decommissioned
  table doesn't show up as perpetually stale forever. Built entirely on
  `metadata/client.py` (Phase 15) — this module owns no tables or state of
  its own, only the health-status logic.

Wired into `airflow/dags/health_check_dag.py`: a `@daily` DAG, independent
of the ingest/silver/gold DAGs (no `ExternalTaskSensor` — health checking
should keep working even when today's actual pipeline run is broken, that's
the point of it), that fails loudly (`AirflowException`, picked up by the
same `on_failure_alert` callback every other DAG uses) if any table in its
expected-tables list comes back unhealthy.
