from datetime import date

from data_quality.runner import (
    validate_bronze_customers,
    validate_silver_customers,
    validate_silver_order_lines,
    validate_silver_orders,
)
from moto import mock_aws

from tests.data_quality.helpers import seed_bronze, seed_silver


@mock_aws
def test_validate_silver_customers_passes_on_clean_data():
    batch_date = date(2024, 1, 1)
    seed_silver(
        "customers",
        "customers",
        batch_date,
        [
            {"customer_id": 1, "email": "a@example.com", "loyalty_tier": "gold"},
            {"customer_id": 2, "email": "b@example.com", "loyalty_tier": "silver"},
        ],
    )

    result = validate_silver_customers(batch_date)

    assert result.success is True
    assert result.row_count == 2


@mock_aws
def test_validate_silver_customers_fails_on_duplicate_id_and_bad_email():
    batch_date = date(2024, 1, 1)
    seed_silver(
        "customers",
        "customers",
        batch_date,
        [
            {"customer_id": 1, "email": "a@example.com", "loyalty_tier": "gold"},
            {"customer_id": 1, "email": "not-an-email", "loyalty_tier": "platinum"},
        ],
    )

    result = validate_silver_customers(batch_date)

    assert result.success is False


@mock_aws
def test_validate_silver_orders_fails_on_unknown_status():
    batch_date = date(2024, 1, 1)
    seed_silver(
        "sales",
        "orders",
        batch_date,
        [{"order_id": 100, "customer_id": 1, "order_status": "not_a_real_status"}],
    )

    result = validate_silver_orders(batch_date)

    assert result.success is False


@mock_aws
def test_validate_silver_order_lines_catches_orphaned_order_id():
    batch_date = date(2024, 1, 1)
    seed_silver(
        "sales",
        "orders",
        batch_date,
        [{"order_id": 100, "customer_id": 1, "order_status": "completed"}],
    )
    seed_silver(
        "sales_order_lines",
        "order_lines",
        batch_date,
        [
            {"order_line_id": 1, "order_id": 100, "quantity": 2, "unit_price": 9.99},
            {"order_line_id": 2, "order_id": 999, "quantity": 1, "unit_price": 5.00},  # orphan
        ],
    )

    result = validate_silver_order_lines(batch_date)

    assert result.success is False


@mock_aws
def test_validate_silver_order_lines_passes_when_all_orders_exist():
    batch_date = date(2024, 1, 1)
    seed_silver(
        "sales",
        "orders",
        batch_date,
        [{"order_id": 100, "customer_id": 1, "order_status": "completed"}],
    )
    seed_silver(
        "sales_order_lines",
        "order_lines",
        batch_date,
        [{"order_line_id": 1, "order_id": 100, "quantity": 2, "unit_price": 9.99}],
    )

    result = validate_silver_order_lines(batch_date)

    assert result.success is True


@mock_aws
def test_validate_bronze_customers_checks_schema_only():
    batch_date = date(2024, 1, 1)
    seed_bronze(
        "customers",
        "customers",
        batch_date,
        [
            {
                "customer_id": 1,
                "email": "a@example.com",
                "first_name": "Alex",
                "last_name": "Smith",
                "signup_date": "2024-01-01",
                "loyalty_tier": "gold",
                "phone": None,
            }
        ],
    )

    result = validate_bronze_customers(batch_date)

    assert result.success is True
