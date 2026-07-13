from datetime import date

import pytest
from metadata.schema import create_all, lineage, table_ownership
from metadata.schema import to_connectable as _connectable
from sqlalchemy import create_engine

from metadata import client


@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:")
    create_all(eng)
    return _connectable(eng)


def test_start_and_complete_run_records_success(engine):
    run_id = client.start_run(
        engine, "customers_silver", "silver", "customers", "customers", date(2024, 1, 1)
    )
    client.complete_run(engine, run_id, status="success", row_count=42)

    with engine.connect() as conn:
        from metadata.schema import pipeline_runs

        row = conn.execute(pipeline_runs.select()).mappings().first()

    assert row["run_id"] == run_id
    assert row["status"] == "success"
    assert row["row_count"] == 42
    assert row["finished_at"] is not None


def test_track_run_success_updates_freshness(engine):
    with client.track_run(
        engine, "customers_silver", "silver", "customers", "customers", date(2024, 1, 1)
    ) as run:
        run.row_count = 10

    freshness = client.get_freshness(engine, "silver", "customers")
    assert freshness["last_row_count"] == 10
    assert freshness["last_successful_batch_date"] == date(2024, 1, 1)


def test_track_run_failure_records_error_and_reraises(engine):
    with (
        pytest.raises(ValueError, match="boom"),
        client.track_run(
            engine, "customers_silver", "silver", "customers", "customers", date(2024, 1, 1)
        ),
    ):
        raise ValueError("boom")

    with engine.connect() as conn:
        from metadata.schema import pipeline_runs

        row = conn.execute(pipeline_runs.select()).mappings().first()

    assert row["status"] == "failed"
    assert "boom" in row["error_message"]
    # A failed run must not silently mark the table as fresh.
    assert client.get_freshness(engine, "silver", "customers") is None


def test_record_schema_version_detects_drift(engine):
    changed = client.record_schema_version(engine, "silver", "customers", ["customer_id", "email"])
    assert changed is True

    unchanged = client.record_schema_version(
        engine, "silver", "customers", ["email", "customer_id"]
    )
    assert unchanged is False  # same columns, different order -> not a real change

    changed_again = client.record_schema_version(
        engine, "silver", "customers", ["customer_id", "email", "phone"]
    )
    assert changed_again is True


def test_track_run_with_columns_records_schema_version(engine):
    with client.track_run(
        engine, "customers_silver", "silver", "customers", "customers", date(2024, 1, 1)
    ) as run:
        run.row_count = 5
        run.columns = ["customer_id", "email"]

    changed = client.record_schema_version(engine, "silver", "customers", ["customer_id", "email"])
    assert changed is False  # already recorded by track_run above


def test_get_lineage_and_ownership_query_seeded_rows(engine):
    with engine.begin() as conn:
        conn.execute(
            lineage.insert().values(
                layer="gold",
                table_name="fact_sales",
                upstream_layer="silver",
                upstream_table_name="orders",
            )
        )
        conn.execute(
            table_ownership.insert().values(
                layer="gold",
                table_name="fact_sales",
                owner_team="data-platform",
                owner_contact=None,
                description=None,
            )
        )

    edges = client.get_lineage(engine, "gold", "fact_sales")
    assert len(edges) == 1
    assert edges[0]["upstream_table_name"] == "orders"

    owner = client.get_ownership(engine, "gold", "fact_sales")
    assert owner["owner_team"] == "data-platform"


def test_get_freshness_returns_none_when_never_recorded(engine):
    assert client.get_freshness(engine, "silver", "nonexistent_table") is None
