import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from spark.jobs.stores_silver import clean  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test-stores-silver").master("local[1]").getOrCreate()
    yield session
    session.stop()


def test_clean_casts_types_and_drops_missing_store_id(spark):
    rows = [
        {
            "store_id": "1",
            "store_name": "Downtown Flagship",
            "region": "Northeast",
            "country": "USA",
            "opened_date": "2015-03-01",
            "_ingested_at": "2024-01-01T00:00:00",
        },
        {
            "store_id": None,
            "store_name": "Orphan Store",
            "region": "West",
            "country": "USA",
            "opened_date": "2020-01-01",
            "_ingested_at": "2024-01-01T00:00:00",
        },
    ]
    df = spark.createDataFrame(rows)

    result = clean(df).collect()

    assert len(result) == 1
    assert result[0]["store_id"] == 1
    assert str(result[0]["opened_date"]) == "2015-03-01"
