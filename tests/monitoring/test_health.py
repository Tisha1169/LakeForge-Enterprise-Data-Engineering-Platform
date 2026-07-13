from datetime import UTC, date, datetime, timedelta

import pytest
from metadata.client import complete_run, start_run, upsert_freshness
from metadata.schema import create_all
from metadata.schema import to_connectable as _connectable
from monitoring.health import check_platform_health, check_table_health
from sqlalchemy import create_engine


@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:")
    create_all(eng)
    return _connectable(eng)


NOW = datetime(2024, 6, 15, 12, 0, tzinfo=UTC)


def test_never_run_table_reports_never_run(engine):
    health = check_table_health(engine, "silver", "customers", now=NOW)
    assert health.status == "never_run"


def test_recently_updated_table_with_no_failures_is_healthy(engine):
    upsert_freshness(engine, "silver", "customers", date(2024, 6, 15), row_count=10)

    health = check_table_health(engine, "silver", "customers", now=NOW)

    assert health.status == "healthy"
    assert health.recent_failure_count == 0


def test_stale_table_beyond_threshold_reports_stale(engine):
    old_time = NOW - timedelta(hours=48)
    with engine.begin() as conn:
        from metadata.schema import table_freshness

        conn.execute(
            table_freshness.insert().values(
                layer="silver",
                table_name="customers",
                last_successful_batch_date=date(2024, 6, 13),
                last_updated_at=old_time,
                last_row_count=10,
            )
        )

    health = check_table_health(engine, "silver", "customers", max_staleness_hours=30, now=NOW)

    assert health.status == "stale"
    assert health.hours_since_update == 48.0


def test_recent_failure_overrides_freshness_and_reports_failing(engine):
    upsert_freshness(engine, "silver", "customers", date(2024, 6, 15), row_count=10)
    run_id = start_run(
        engine, "customers_silver", "silver", "customers", "customers", date(2024, 6, 15)
    )
    complete_run(engine, run_id, status="failed", error_message="boom")

    health = check_table_health(
        engine, "silver", "customers", failure_lookback_hours=24, now=NOW + timedelta(minutes=1)
    )

    assert health.status == "failing"
    assert health.recent_failure_count == 1


def test_old_failure_outside_lookback_window_does_not_count(engine):
    upsert_freshness(engine, "silver", "customers", date(2024, 6, 15), row_count=10)
    with engine.begin() as conn:
        from metadata.schema import pipeline_runs

        conn.execute(
            pipeline_runs.insert().values(
                run_id="old-failed-run",
                pipeline_name="customers_silver",
                layer="silver",
                source_name="customers",
                table_name="customers",
                batch_date=date(2024, 6, 1),
                status="failed",
                started_at=NOW - timedelta(days=10),
                error_message="old failure",
            )
        )

    health = check_table_health(engine, "silver", "customers", failure_lookback_hours=24, now=NOW)

    assert health.status == "healthy"
    assert health.recent_failure_count == 0


def test_check_platform_health_aggregates_multiple_tables(engine):
    upsert_freshness(engine, "silver", "customers", date(2024, 6, 15), row_count=10)
    # "orders" is never run.

    results = check_platform_health(
        engine, [("silver", "customers"), ("silver", "orders")], now=NOW
    )

    assert len(results) == 2
    by_table = {r.table_name: r for r in results}
    assert by_table["customers"].status == "healthy"
    assert by_table["orders"].status == "never_run"
