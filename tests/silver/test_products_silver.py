import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from spark.jobs.products_silver import clean  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test-products-silver").master("local[1]").getOrCreate()
    yield session
    session.stop()


def test_clean_casts_and_drops_missing_sku(spark):
    rows = [
        {
            "product_id": "101",
            "sku": "SKU-101",
            "product_name": "Wireless Mouse",
            "category": "Electronics",
            "unit_price": "24.99",
            "_ingested_at": "2024-01-01T00:00:00",
        },
        {
            "product_id": "102",
            "sku": "",
            "product_name": "Missing SKU",
            "category": "Electronics",
            "unit_price": "5.00",
            "_ingested_at": "2024-01-01T00:00:00",
        },
    ]
    df = spark.createDataFrame(rows)

    result = clean(df).collect()

    assert len(result) == 1
    assert result[0]["product_id"] == 101
    assert result[0]["unit_price"] == 24.99
