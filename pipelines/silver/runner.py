"""Thin orchestration around the Spark Silver jobs — parameter resolution
and job lookup only. The actual transformation logic lives in `spark/jobs/`.
Airflow tasks (Phase 11) call `run_silver_job`; this has no Airflow
dependency itself, so it's callable directly for local testing too.
"""

from __future__ import annotations

from datetime import date

from spark.jobs import (
    customers_silver,
    inventory_silver,
    products_silver,
    sales_order_lines_silver,
    sales_silver,
    stores_silver,
    suppliers_silver,
)

_JOBS = {
    "customers": customers_silver,
    "sales_orders": sales_silver,
    "sales_order_lines": sales_order_lines_silver,
    "products": products_silver,
    "stores": stores_silver,
    "inventory": inventory_silver,
    "suppliers": suppliers_silver,
}


def run_silver_job(job_name: str, batch_date: date) -> None:
    if job_name not in _JOBS:
        raise ValueError(f"Unknown Silver job '{job_name}'. Known jobs: {sorted(_JOBS)}")
    _JOBS[job_name].run(batch_date)
