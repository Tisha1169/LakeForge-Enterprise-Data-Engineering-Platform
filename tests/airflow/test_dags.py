"""Real DagBag-based structural tests for the DAGs in airflow/dags/.

Skipped entirely if apache-airflow isn't installed (it's a heavy, separately
-installed dependency — see docker/airflow/requirements.txt — not part of
the core project deps every other test relies on). Airflow 2.x (what
docker/airflow/Dockerfile actually pins) has no Python 3.13 build as of this
writing, so this runs against whatever Airflow version is available
locally; see airflow/README.md for the same caveat applied during manual
Phase 11 validation.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Bare "airflow" isn't a safe importorskip target here: this project's own
# airflow/ directory (no __init__.py) becomes a namespace package for the
# top-level name "airflow" whenever the real apache-airflow isn't installed,
# so `pytest.importorskip("airflow")` doesn't actually skip — reproduced
# directly (ModuleNotFoundError: No module named 'airflow.models' even
# though bare `import airflow` "succeeded"). airflow.models only exists in
# the real package, so skip on that instead.
pytest.importorskip("airflow.models")

from airflow.models import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).parent.parent.parent / "airflow" / "dags"


@pytest.fixture(scope="module")
def dagbag():
    # Airflow DAG files do `from common import ...`, assuming the dags
    # folder itself is on sys.path (how Airflow's own DAG processor adds
    # it) — replicate that here rather than changing the DAG files.
    sys.path.insert(0, str(DAGS_DIR.parent.parent))
    sys.path.insert(0, str(DAGS_DIR))
    return DagBag(dag_folder=str(DAGS_DIR))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_all_four_dags_are_discovered(dagbag):
    assert set(dagbag.dags.keys()) == {
        "openlake_bronze_ingestion",
        "openlake_silver_transform",
        "openlake_gold_build",
        "openlake_health_check",
    }


def test_every_dag_has_retries_configured(dagbag):
    for dag_id, dag in dagbag.dags.items():
        for task in dag.tasks:
            assert task.retries and task.retries > 0, (
                f"{dag_id}.{task.task_id} has no retries configured"
            )


def test_bronze_ingestion_dag_gates_every_source_behind_preflight(dagbag):
    dag = dagbag.dags["openlake_bronze_ingestion"]
    preflight = dag.get_task("preflight_check_source_db")
    # TaskGroup prefixes task_id with the group name (e.g.
    # "source__customers.cadence_gate"), so match on suffix.
    gate_tasks = [t for t in dag.tasks if t.task_id.endswith(".cadence_gate")]

    assert len(gate_tasks) == 7  # one per configured source
    for gate in gate_tasks:
        assert preflight.task_id in [u.task_id for u in gate.upstream_list]


def test_silver_transform_dag_orders_depends_before_order_lines(dagbag):
    dag = dagbag.dags["openlake_silver_transform"]
    order_lines_task = dag.get_task("silver__sales_order_lines")
    upstream_ids = [t.task_id for t in order_lines_task.upstream_list]

    assert upstream_ids == ["silver__sales_orders"]


def test_gold_build_dag_waits_for_silver_transform(dagbag):
    dag = dagbag.dags["openlake_gold_build"]
    sensor = dag.get_task("wait_for_silver_transform")

    assert sensor.external_dag_id == "openlake_silver_transform"


def test_health_check_dag_is_not_chained_to_other_dags(dagbag):
    """Health checking is deliberately independent — see
    monitoring/README.md for why."""
    dag = dagbag.dags["openlake_health_check"]

    sensor_tasks = [t for t in dag.tasks if "Sensor" in type(t).__name__]
    assert sensor_tasks == []
