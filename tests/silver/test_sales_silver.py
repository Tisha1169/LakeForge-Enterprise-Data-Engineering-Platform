import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from spark.jobs.sales_silver import clean  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test-sales-silver").master("local[1]").getOrCreate()
    yield session
    session.stop()


def test_clean_dedupes_by_latest_updated_ts_and_flags_unknown_status(spark):
    rows = [
        {
            "order_id": "1",
            "customer_id": "10",
            "store_id": "1",
            "order_status": "pending",
            "order_ts": "2024-01-01 08:00:00",
            "updated_ts": "2024-01-01 08:10:00",
        },
        {
            "order_id": "1",  # same order, updated later -> this row should win
            "customer_id": "10",
            "store_id": "1",
            "order_status": "completed",
            "order_ts": "2024-01-01 08:00:00",
            "updated_ts": "2024-01-01 09:00:00",
        },
        {
            "order_id": "2",
            "customer_id": "11",
            "store_id": "2",
            "order_status": "weird_status",  # not in the known set
            "order_ts": "2024-01-01 10:00:00",
            "updated_ts": "2024-01-01 10:05:00",
        },
    ]
    df = spark.createDataFrame(rows)

    result = clean(df).orderBy("order_id").collect()

    assert len(result) == 2
    assert result[0]["order_id"] == 1
    assert result[0]["order_status"] == "completed"  # the later-updated row won dedup
    assert result[1]["order_status"] == "unknown"
