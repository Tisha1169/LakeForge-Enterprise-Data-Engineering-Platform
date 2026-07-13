import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from spark.jobs.sales_order_lines_silver import clean  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("test-sales-order-lines-silver")
        .master("local[1]")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_clean_casts_numeric_types_and_drops_missing_grain(spark):
    rows = [
        {
            "order_line_id": "1",
            "order_id": "100",
            "product_id": "101",
            "quantity": "2",
            "unit_price": "24.99",
            "discount_pct": "0",
            "_ingested_at": "2024-01-01T00:00:00",
        },
        {
            "order_line_id": None,  # missing grain -> dropped
            "order_id": "100",
            "product_id": "102",
            "quantity": "1",
            "unit_price": "9.99",
            "discount_pct": "0",
            "_ingested_at": "2024-01-01T00:00:00",
        },
    ]
    df = spark.createDataFrame(rows)
    valid_orders_df = spark.createDataFrame([{"order_id": 100}])

    result = clean(df, valid_orders_df).collect()

    assert len(result) == 1
    row = result[0]
    assert row["order_line_id"] == 1
    assert row["quantity"] == 2
    assert row["unit_price"] == 24.99


def test_clean_drops_order_lines_with_no_matching_order(spark):
    rows = [
        {
            "order_line_id": "1",
            "order_id": "100",
            "product_id": "101",
            "quantity": "2",
            "unit_price": "24.99",
            "discount_pct": "0",
            "_ingested_at": "2024-01-01T00:00:00",
        },
        {
            "order_line_id": "2",
            "order_id": "999",  # no matching order -> dropped by the broadcast semi-join
            "product_id": "102",
            "quantity": "1",
            "unit_price": "9.99",
            "discount_pct": "0",
            "_ingested_at": "2024-01-01T00:00:00",
        },
    ]
    df = spark.createDataFrame(rows)
    valid_orders_df = spark.createDataFrame([{"order_id": 100}])

    result = clean(df, valid_orders_df).collect()

    assert len(result) == 1
    assert result[0]["order_line_id"] == 1
