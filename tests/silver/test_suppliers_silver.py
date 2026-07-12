import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from spark.jobs.suppliers_silver import clean  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test-suppliers-silver").master("local[1]").getOrCreate()
    yield session
    session.stop()


def test_clean_drops_missing_sku_and_parses_both_date_formats(spark):
    rows = [
        {
            "supplier_id": "2",
            "product_sku": "SKU-103",
            "unit_cost": "14.50",
            "lead_time_days": "21",
            "updated_at": None,  # genuinely missing -> stays null, not guessed
            "_ingested_at": "2024-01-01T00:00:00",
        },
        {
            "supplier_id": "3",
            "product_sku": "SKU-104",
            "unit_cost": "210.00",
            "lead_time_days": "14",
            "updated_at": "01/03/2024",  # MM/dd/yyyy
            "_ingested_at": "2024-01-01T00:00:00",
        },
        {
            "supplier_id": "4",
            "product_sku": "",  # missing SKU -> dropped
            "unit_cost": "6.00",
            "lead_time_days": "18",
            "updated_at": "2024-01-04",
            "_ingested_at": "2024-01-01T00:00:00",
        },
    ]
    df = spark.createDataFrame(rows)

    result = clean(df).orderBy("supplier_id").collect()

    assert len(result) == 2
    assert result[0]["updated_at"] is None
    assert str(result[1]["updated_at"]) == "2024-01-03"
