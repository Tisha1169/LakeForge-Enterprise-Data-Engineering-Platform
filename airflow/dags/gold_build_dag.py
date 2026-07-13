"""Silver -> Gold. A single task, deliberately not split per Gold table:
splitting it would mean passing full dimension row-lists between tasks via
XCom, which is meant for small metadata, not bulk data. `run_gold_build`
already writes each table to the Gold bucket as it completes, so if
splitting into separate tasks is ever needed for parallelism, each task
should re-*read* its inputs from Gold rather than receive them via XCom.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from common import DEFAULT_ARGS, on_failure_alert


@task(task_id="build_gold")
def build_gold_task(ds: str) -> dict[str, int]:
    from datetime import date

    from pipelines.gold.runner import run_gold_build

    return run_gold_build(date.fromisoformat(ds))


with DAG(
    dag_id="openlake_gold_build",
    description="Build the Gold star schema (dims, fact_sales, daily_sales_summary) from Silver.",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    on_failure_callback=on_failure_alert,
    tags=["openlake", "gold", "star-schema"],
) as dag:
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver_transform",
        external_dag_id="openlake_silver_transform",
        # See silver_transform_dag.py for why "upstream_failed" isn't valid
        # here (waiting on a whole DagRun, not one specific task).
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        timeout=timedelta(hours=2),
        poke_interval=60,
    )

    wait_for_silver >> build_gold_task("{{ ds }}")
