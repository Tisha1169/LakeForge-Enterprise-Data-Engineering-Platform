"""Bronze -> Silver, one task per Spark cleaning job. Waits for the Bronze
DAG's same logical date to finish via ExternalTaskSensor before starting —
kept as a separate DAG (rather than one giant DAG) so Bronze can be
backfilled/rerun independently of Silver.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from common import DEFAULT_ARGS, on_failure_alert

SILVER_JOBS = [
    "customers",
    "sales_orders",
    "sales_order_lines",
    "products",
    "stores",
    "inventory",
    "suppliers",
]


@task(task_id="run_silver_job")
def run_silver_job_task(job_name: str, ds: str) -> None:
    from datetime import date

    from pipelines.silver.runner import run_silver_job

    run_silver_job(job_name, date.fromisoformat(ds))


with DAG(
    dag_id="openlake_silver_transform",
    description="Clean/cast/dedup every Bronze table into Silver via PySpark.",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    on_failure_callback=on_failure_alert,
    tags=["openlake", "silver", "pyspark"],
) as dag:
    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze_ingestion",
        external_dag_id="openlake_bronze_ingestion",
        # Waiting on the whole external DAG run (no external_task_id), so
        # only DagRunState values are valid here — "upstream_failed" is a
        # TaskInstanceState concept, only meaningful when waiting on one
        # specific task within the external DAG.
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        timeout=timedelta(hours=2),
        poke_interval=60,
    )

    for job_name in SILVER_JOBS:
        wait_for_bronze >> run_silver_job_task.override(task_id=f"silver__{job_name}")(
            job_name, "{{ ds }}"
        )
