"""Daily platform health check: is every table the platform maintains
fresh and failure-free? Runs independently of the ingest/silver/gold DAGs
(no ExternalTaskSensor here) — health checking should keep working even if
today's pipeline run itself is broken, that's the point of it.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from common import DEFAULT_ARGS, on_failure_alert

# The set of tables the platform is expected to be maintaining — see
# monitoring/health.py for why this is explicit rather than derived from
# whatever metadata happens to contain.
EXPECTED_TABLES = [
    ("bronze", "customers"),
    ("silver", "customers"),
    ("silver", "orders"),
    ("silver", "order_lines"),
    ("silver", "products"),
    ("silver", "stores"),
    ("gold", "dim_customer"),
    ("gold", "dim_product"),
    ("gold", "dim_store"),
    ("gold", "fact_sales"),
]


@task(task_id="check_platform_health")
def check_platform_health_task() -> None:
    from metadata.client import default_engine
    from monitoring.health import check_platform_health

    results = check_platform_health(default_engine(), EXPECTED_TABLES)
    unhealthy = [r for r in results if not r.is_healthy]
    if unhealthy:
        summary = "; ".join(
            f"{r.layer}.{r.table_name}: {r.status} ({r.message})" for r in unhealthy
        )
        raise AirflowException(f"{len(unhealthy)} unhealthy table(s): {summary}")


with DAG(
    dag_id="openlake_health_check",
    description="Checks every expected table's freshness and recent-failure status.",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    on_failure_callback=on_failure_alert,
    tags=["openlake", "monitoring"],
) as dag:
    check_platform_health_task()
