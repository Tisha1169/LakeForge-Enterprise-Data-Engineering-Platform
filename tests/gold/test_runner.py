from datetime import date

from moto import mock_aws
from pipelines.gold.runner import run_gold_build
from pipelines.gold.writer import read_gold_table
from pipelines.storage import LakeLayer, ensure_bucket

from tests.gold.helpers import seed_silver


@mock_aws
def test_run_gold_build_produces_all_tables_end_to_end():
    ensure_bucket(LakeLayer.GOLD)
    batch_date = date(2024, 1, 1)

    seed_silver(
        "customers",
        "customers",
        batch_date,
        [
            {
                "customer_id": 1,
                "email": "a@example.com",
                "first_name": "Alex",
                "last_name": "Smith",
                "loyalty_tier": "gold",
            }
        ],
    )
    seed_silver(
        "sales_products",
        "products",
        batch_date,
        [
            {
                "product_id": 101,
                "sku": "SKU-101",
                "product_name": "Mouse",
                "category": "Electronics",
                "unit_price": 10.0,
            }
        ],
    )
    seed_silver(
        "sales_stores",
        "stores",
        batch_date,
        [
            {
                "store_id": 1,
                "store_name": "Downtown",
                "region": "NE",
                "country": "USA",
                "opened_date": date(2015, 1, 1),
            }
        ],
    )
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
                "order_ts": "2024-01-01 09:00:00",
                "updated_ts": "2024-01-01 09:10:00",
            }
        ],
    )

    counts = run_gold_build(batch_date)

    assert counts["dim_customer"] == 1
    assert counts["dim_product"] == 1
    assert counts["dim_store"] == 1
    assert counts["fact_sales"] == 1
    assert counts["daily_sales_summary"] == 1
    assert counts["dim_date"] > 365  # multi-year generated calendar

    fact_rows = read_gold_table("fact_sales")
    assert fact_rows[0]["extended_amount"] == 20.0
