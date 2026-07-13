from datetime import date

from moto import mock_aws
from pipelines.gold.dim_store import build_dim_store

from tests.gold.helpers import seed_silver


@mock_aws
def test_build_dim_store_maps_fields_and_generates_surrogate_key():
    batch_date = date(2024, 1, 1)
    seed_silver(
        "sales_stores",
        "stores",
        batch_date,
        [
            {
                "store_id": 1,
                "store_name": "Downtown Flagship",
                "region": "Northeast",
                "country": "USA",
                "opened_date": date(2015, 3, 1),
            }
        ],
    )

    rows = build_dim_store(batch_date)

    assert len(rows) == 1
    assert rows[0]["store_id"] == 1
    assert rows[0]["store_sk"] is not None
