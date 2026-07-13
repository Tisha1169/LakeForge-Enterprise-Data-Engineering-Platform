"""Raw sources -> Landing -> Bronze, one task pair per configured source.

Runs `@daily`. Sources with a non-daily cadence (suppliers, `@weekly`) still
get evaluated every run but skip via ShortCircuitOperator on non-matching
days — see `common.should_run_source`.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from common import (
    DEFAULT_ARGS,
    SOURCE_TABLE_NAMES,
    on_failure_alert,
    run_bronze,
    run_ingestion,
    should_run_source,
)


@task(task_id="preflight_check_source_db")
def preflight_check_source_db() -> None:
    """Demonstrates Airflow Connections: fails fast (before any ingestion
    starts) if the source Postgres isn't reachable, via the
    `openlake_source_db` Connection registered from AIRFLOW_CONN_
    OPENLAKE_SOURCE_DB in docker-compose.yml — rather than every DB source
    task discovering that independently a few minutes into the run."""
    hook = PostgresHook(postgres_conn_id="openlake_source_db")
    hook.get_first("SELECT 1")


@task(task_id="ingest")
def ingest_task(source_name: str, ds: str) -> str:
    return run_ingestion(source_name, ds)


@task(task_id="to_bronze")
def bronze_task(source_name: str, table_name: str, ds: str) -> str:
    return run_bronze(source_name, table_name, ds)


with DAG(
    dag_id="openlake_bronze_ingestion",
    description="Ingest all configured sources into the landing zone, then land them as Bronze Parquet.",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    on_failure_callback=on_failure_alert,
    tags=["openlake", "bronze", "ingestion"],
) as dag:
    preflight = preflight_check_source_db()

    for source_name, table_name in SOURCE_TABLE_NAMES.items():
        with TaskGroup(group_id=f"source__{source_name}"):
            gate = ShortCircuitOperator(
                task_id="cadence_gate",
                python_callable=should_run_source,
                op_kwargs={"source_name": source_name, "batch_date_str": "{{ ds }}"},
            )
            (
                preflight
                >> gate
                >> ingest_task(source_name, "{{ ds }}")
                >> bronze_task(source_name, table_name, "{{ ds }}")
            )
