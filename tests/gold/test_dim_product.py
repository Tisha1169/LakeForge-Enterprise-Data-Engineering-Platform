from datetime import date

from moto import mock_aws
from pipelines.gold.dim_product import build_dim_product

from tests.gold.helpers import seed_silver


@mock_aws
def test_build_dim_product_maps_fields_and_generates_surrogate_key():
    batch_date = date(2024, 1, 1)
    seed_silver(
        "sales_products",
        "products",
        batch_date,
        [
            {
                "product_id": 101,
                "sku": "SKU-101",
                "product_name": "Wireless Mouse",
                "category": "Electronics",
                "unit_price": 24.99,
            }
        ],
    )

    rows = build_dim_product(batch_date)

    assert len(rows) == 1
    assert rows[0]["product_id"] == 101
    assert rows[0]["product_sk"] is not None
    assert rows[0]["unit_price"] == 24.99


@mock_aws
def test_build_dim_product_returns_empty_when_no_silver_data():
    assert build_dim_product(date(2024, 1, 1)) == []
