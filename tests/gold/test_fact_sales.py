from datetime import date

from moto import mock_aws
from pipelines.gold.fact_sales import build_fact_sales

from tests.gold.helpers import seed_silver


@mock_aws
def test_build_fact_sales_joins_dims_and_computes_extended_amount():
    batch_date = date(2024, 1, 1)
    seed_silver(
        "sales_order_lines",
        "order_lines",
        batch_date,
        [
            {
                "order_line_id": 1,
                "order_id": 100,
                "product_id": 101,
                "quantity": 2,
                "unit_price": 10.0,
                "discount_pct": 10.0,
            }
        ],
    )
    seed_silver(
        "sales",
        "orders",
        batch_date,
        [
            {
                "order_id": 100,
                "customer_id": 1,
                "store_id": 1,
                "order_status": "completed",
                "order_ts": "2024-01-01 09:00:00",
                "updated_ts": "2024-01-01 09:10:00",
            }
        ],
    )

    dim_customer_rows = [
        {
            "customer_sk": 111,
            "customer_id": 1,
            "email": "a@example.com",
            "first_name": "Alex",
            "last_name": "Smith",
            "loyalty_tier": "gold",
            "effective_date": date(2024, 1, 1),
            "end_date": None,
            "is_current": True,
        }
    ]
    dim_product_rows = [
        {
            "product_sk": 222,
            "product_id": 101,
            "sku": "SKU-101",
            "product_name": "Mouse",
            "category": "Electronics",
            "unit_price": 10.0,
        }
    ]
    dim_store_rows = [
        {
            "store_sk": 333,
            "store_id": 1,
            "store_name": "Downtown",
            "region": "NE",
            "country": "USA",
            "opened_date": date(2015, 1, 1),
        }
    ]
    dim_date_rows = [
        {
            "date_key": 20240101,
            "full_date": date(2024, 1, 1),
            "year": 2024,
            "quarter": 1,
            "month": 1,
            "month_name": "January",
            "day": 1,
            "day_of_week": 1,
            "day_name": "Monday",
            "is_weekend": False,
        }
    ]

    rows = build_fact_sales(
        batch_date, dim_customer_rows, dim_product_rows, dim_store_rows, dim_date_rows
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["customer_sk"] == 111
    assert row["product_sk"] == 222
    assert row["store_sk"] == 333
    assert row["date_key"] == 20240101
    # 2 * 10.0 * (1 - 10/100) = 18.0
    assert row["extended_amount"] == 18.0


@mock_aws
def test_build_fact_sales_picks_customer_version_effective_at_order_time():
    batch_date = date(2024, 1, 10)
    seed_silver(
        "sales_order_lines",
        "order_lines",
        batch_date,
        [
            {
                "order_line_id": 1,
                "order_id": 100,
                "product_id": 101,
                "quantity": 1,
                "unit_price": 5.0,
                "discount_pct": 0,
            }
        ],
    )
    seed_silver(
        "sales",
        "orders",
        batch_date,
        [
            {
                "order_id": 100,
                "customer_id": 1,
                "store_id": 1,
                "order_status": "completed",
                "order_ts": "2024-01-03 09:00:00",  # falls in the FIRST customer version's window
                "updated_ts": "2024-01-03 09:10:00",
            }
        ],
    )

    dim_customer_rows = [
        {
            "customer_sk": 111,
            "customer_id": 1,
            "email": "a@example.com",
            "first_name": "Alex",
            "last_name": "Smith",
            "loyalty_tier": "bronze",
            "effective_date": date(2024, 1, 1),
            "end_date": date(2024, 1, 7),
            "is_current": False,
        },
        {
            "customer_sk": 999,
            "customer_id": 1,
            "email": "a@example.com",
            "first_name": "Alex",
            "last_name": "Smith",
            "loyalty_tier": "gold",
            "effective_date": date(2024, 1, 8),
            "end_date": None,
            "is_current": True,
        },
    ]

    rows = build_fact_sales(batch_date, dim_customer_rows, [], [], [])

    assert len(rows) == 1
    assert rows[0]["customer_sk"] == 111  # the bronze-tier version, not the current gold-tier one
