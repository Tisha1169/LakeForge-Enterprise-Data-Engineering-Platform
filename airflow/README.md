# airflow/

Orchestration layer. Owns *when* and *in what order* pipelines run — contains
no business/transformation logic itself, only calls into `pipelines/` and
`spark/`.

## DAGs

Four DAGs — one per medallion layer, chained via `ExternalTaskSensor` (not
one giant DAG) so each layer can be backfilled or rerun independently
without forcing the others to rerun too, plus an independent health check:

- `dags/common.py` — shared constants/helpers (not a DAG file itself;
  Airflow's DagBag ignores modules with no DAG object). `DEFAULT_ARGS`
  (retries, exponential backoff), `on_failure_alert` (structured-log failure
  callback — a stand-in for a real Slack/PagerDuty integration this project
  doesn't have credentials for), `should_run_source` (cadence gating), and
  the actual task-body functions (`run_ingestion`, `run_bronze`).
- `dags/bronze_ingestion_dag.py` — `openlake_bronze_ingestion`, `@daily`. A
  `preflight_check_source_db` task (via `PostgresHook`, demonstrating Airflow
  Connections — registered from `AIRFLOW_CONN_OPENLAKE_SOURCE_DB` in
  `docker-compose.yml`) gates a `TaskGroup` per configured source: cadence
  gate (`ShortCircuitOperator` — skips `suppliers` on non-Monday runs, since
  it's configured `@weekly` but the DAG itself runs daily) -> ingest -> Bronze.
- `dags/silver_transform_dag.py` — `openlake_silver_transform`, `@daily`.
  Waits for `openlake_bronze_ingestion`'s same logical date via
  `ExternalTaskSensor`, then fans out one task per Spark Silver job.
- `dags/gold_build_dag.py` — `openlake_gold_build`, `@daily`. Waits for
  `openlake_silver_transform`, then a single `build_gold` task. Deliberately
  *not* split per Gold table — that would mean passing full dimension
  row-lists between tasks via XCom, which is meant for small metadata, not
  bulk data; `run_gold_build` already persists each table to the Gold bucket
  as it completes.
- `dags/health_check_dag.py` — `openlake_health_check`, `@daily`,
  intentionally *not* chained to the other three DAGs — health checking
  should keep working even when today's actual pipeline run is broken.
  Runs `monitoring.health.check_platform_health` against an explicit list
  of expected `(layer, table_name)` pairs and raises `AirflowException`
  (picked up by the same `on_failure_alert` callback) if any come back
  `stale`, `failing`, or `never_run`.

Validated with a real `DagBag` parse (`from airflow.models import DagBag`)
against Airflow 3.3 locally — Airflow 2.x (what `docker/airflow/Dockerfile`
actually pins, 2.10.3) has no Python 3.13 build, so 3.x was the closest
available local validation; only 2.x/3.x-version-specific deprecation
warnings appeared, no import errors, and each DAG's task graph matched what
was intended. This caught a real bug: `ExternalTaskSensor` waiting on a
whole external DagRun (no `external_task_id`) only accepts `DagRunState`
values for `failed_states` — `"upstream_failed"` is a `TaskInstanceState`
concept and isn't valid there.

- `plugins/` — reserved for custom operators/hooks if a future phase needs
  one; none required yet (`PythonOperator`/`@task`, `ShortCircuitOperator`,
  `ExternalTaskSensor`, and `PostgresHook` cover everything so far).
- `config/` — Airflow-specific configuration beyond what's in
  `docker-compose.yml`'s environment block; empty for now.
