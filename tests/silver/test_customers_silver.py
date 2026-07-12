import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from spark.jobs.customers_silver import clean  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test-customers-silver").master("local[1]").getOrCreate()
    yield session
    session.stop()


def test_clean_casts_dedupes_and_standardizes_dates(spark):
    rows = [
        {
            "customer_id": "1",
            "email": "  A@Example.com ",
            "first_name": " Alex ",
            "last_name": "Smith",
            "signup_date": "2023-02-02",
            "_ingested_at": "2024-01-01T00:00:00",
        },
        {
            "customer_id": "1",  # duplicate customer_id, later ingested -> should win
            "email": "a@example.com",
            "first_name": "Alex",
            "last_name": "Smith",
            "signup_date": "2023/02/02",
            "_ingested_at": "2024-01-02T00:00:00",
        },
        {
            "customer_id": None,  # missing grain -> dropped
            "email": "orphan@example.com",
            "first_name": "No",
            "last_name": "Id",
            "signup_date": "2024-04-01",
            "_ingested_at": "2024-01-01T00:00:00",
        },
        {
            "customer_id": "2",
            "email": "",  # empty -> nulled
            "first_name": "Pat",
            "last_name": "Nguyen",
            "signup_date": "04-11-2024",  # MM-dd-yyyy format
            "_ingested_at": "2024-01-01T00:00:00",
        },
    ]
    df = spark.createDataFrame(rows)

    result = clean(df).orderBy("customer_id").collect()

    assert len(result) == 2  # the null-customer_id row was dropped
    assert result[0]["customer_id"] == 1
    assert result[0]["email"] == "a@example.com"
    assert result[0]["first_name"] == "Alex"
    assert str(result[0]["signup_date"]) == "2023-02-02"

    assert result[1]["customer_id"] == 2
    assert result[1]["email"] is None
    assert str(result[1]["signup_date"]) == "2024-04-11"
