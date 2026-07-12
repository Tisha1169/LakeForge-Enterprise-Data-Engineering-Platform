import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from spark.jobs.inventory_silver import clean  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test-inventory-silver").master("local[1]").getOrCreate()
    yield session
    session.stop()


def test_clean_dedupes_on_grain_and_drops_missing_grain(spark):
    rows = [
        {
            "snapshot_id": "1",
            "store_id": "1",
            "product_id": "101",
            "snapshot_date": "2024-01-01",
            "quantity_on_hand": "50",
            "reorder_point": "10",
            "supplier_id": "1",
            "_ingested_at": "2024-01-01T00:00:00",
        },
        {
            "snapshot_id": "2",  # duplicate grain, ingested later -> should win
            "store_id": "1",
            "product_id": "101",
            "snapshot_date": "2024-01-01",
            "quantity_on_hand": "48",
            "reorder_point": "10",
            "supplier_id": "1",
            "_ingested_at": "2024-01-02T00:00:00",
        },
        {
            "snapshot_id": "3",
            "store_id": None,  # missing grain -> dropped
            "product_id": "102",
            "snapshot_date": "2024-01-01",
            "quantity_on_hand": "5",
            "reorder_point": "10",
            "supplier_id": "1",
            "_ingested_at": "2024-01-01T00:00:00",
        },
    ]
    df = spark.createDataFrame(rows)

    result = clean(df).collect()

    assert len(result) == 1
    assert result[0]["quantity_on_hand"] == 48
