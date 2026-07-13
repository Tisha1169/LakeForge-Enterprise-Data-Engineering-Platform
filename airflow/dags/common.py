"""Shared constants and utilities for OpenLake DAGs — not a DAG file itself
(Airflow's DagBag ignores modules with no DAG object)."""

from __future__ import annotations

from datetime import timedelta

from airflow.exceptions import AirflowException
from config.sources import list_source_configs
from monitoring.logging_config import get_logger
from pipelines.ingestion.api.customer_api import CustomerApiIngestion
from pipelines.ingestion.db.table_extract import DatabaseTableIngestion
from pipelines.ingestion.files.supplier_files import SupplierFileIngestion

logger = get_logger(__name__)

DEFAULT_ARGS = {
    "owner": "openlake-data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# source_type -> ingestion class. "api" only has one real source today
# (CustomerApiIngestion is Customer-API-specific); "db" and "file" are fully
# generic over any configured source, matching the rest of the platform's
# config-driven design.
_INGESTION_CLASSES = {
    "api": CustomerApiIngestion,
    "db": DatabaseTableIngestion,
    "file": SupplierFileIngestion,
}

SOURCE_NAMES = [c.name for c in list_source_configs()]

# Mirrors BaseIngestion.table_name's fallback (config.table or config.name) —
# duplicated here (rather than instantiating an ingestion object) because
# DAG files need this at *parse* time to build the task graph, before any
# task actually runs.
SOURCE_TABLE_NAMES = {c.name: (c.table or c.name) for c in list_source_configs()}


def on_failure_alert(context: dict) -> None:
    """Central failure callback. Logs a structured alert; swap this for a
    real Slack/PagerDuty/email integration when those credentials exist —
    stubbing that out here rather than fabricating a webhook this project
    doesn't actually have configured."""
    task_instance = context["task_instance"]
    logger.error(
        "airflow.task_failed",
        extra={
            "context": {
                "dag_id": task_instance.dag_id,
                "task_id": task_instance.task_id,
                "execution_date": str(context.get("execution_date")),
                "exception": str(context.get("exception")),
            }
        },
    )


def should_run_source(source_name: str, batch_date_str: str) -> bool:
    """Gates a source's tasks against its configured cadence. The DAG itself
    runs `@daily` (so daily sources stay current), but a source configured
    `@weekly` (suppliers) should only actually ingest once a week even
    though the DAG evaluates it every day — this is the ShortCircuitOperator
    condition for that."""
    from datetime import date

    from config.sources import load_source_config

    config = load_source_config(source_name)
    if config.schedule == "@weekly":
        return date.fromisoformat(batch_date_str).isoweekday() == 1  # Monday
    return True


def run_ingestion(source_name: str, batch_date_str: str) -> str:
    """Airflow task body for one source's ingest-to-landing step."""
    from datetime import date

    from config.sources import load_source_config

    config = load_source_config(source_name)
    ingestion_cls = _INGESTION_CLASSES.get(config.source_type)
    if ingestion_cls is None:
        raise AirflowException(
            f"No ingestion class registered for source_type '{config.source_type}'"
        )

    batch_date = date.fromisoformat(batch_date_str)
    ingestion = ingestion_cls(config, batch_date=batch_date)
    result = ingestion.run()
    return result.landing_uri or ""


def run_bronze(source_name: str, table_name: str, batch_date_str: str) -> str:
    """Airflow task body for one source's landing-to-Bronze step."""
    from datetime import date

    from pipelines.bronze.writer import write_bronze

    batch_date = date.fromisoformat(batch_date_str)
    landing_filename = f"{source_name}_{batch_date.isoformat()}.ndjson"
    result = write_bronze(source_name, table_name, batch_date, landing_filename)
    return result.bronze_uri
