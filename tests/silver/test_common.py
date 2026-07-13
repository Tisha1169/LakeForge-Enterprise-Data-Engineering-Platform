from datetime import date

import pytest
from moto import mock_aws

pyspark = pytest.importorskip("pyspark")

from pipelines.bronze.writer import write_bronze  # noqa: E402
from pipelines.silver.reader import read_silver  # noqa: E402
from pipelines.storage import LakeLayer, ObjectKey, ensure_bucket, put_bytes  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from spark.jobs.common import bronze_to_spark_df, silver_to_spark_df, write_silver  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test-common").master("local[1]").getOrCreate()
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def _configure_settings(monkeypatch):
    import pipelines.storage as storage_module

    monkeypatch.setattr(storage_module.settings, "minio_endpoint", "s3.amazonaws.com")
    monkeypatch.setattr(storage_module.settings, "minio_access_key", "testing")
    monkeypatch.setattr(storage_module.settings, "minio_secret_key", "testing")
    monkeypatch.setattr(storage_module.settings, "minio_secure", True)
    monkeypatch.setattr(storage_module.settings, "minio_bucket_landing", "test-landing")
    monkeypatch.setattr(storage_module.settings, "minio_bucket_bronze", "test-bronze")
    monkeypatch.setattr(storage_module.settings, "minio_bucket_silver", "test-silver")


@mock_aws
def test_bronze_to_spark_df_stringifies_mixed_type_column(spark):
    ensure_bucket(LakeLayer.LANDING)
    ensure_bucket(LakeLayer.BRONZE)
    batch_date = date(2024, 1, 1)

    import json

    records = [{"customer_id": 1}, {"customer_id": "2"}]  # mixed types on purpose
    ndjson = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    key = ObjectKey(
        source="customers", table="customers", filename="f.ndjson", batch_date=batch_date
    )
    put_bytes(LakeLayer.LANDING, key, ndjson)
    write_bronze("customers", "customers", batch_date, "f.ndjson")

    df = bronze_to_spark_df(spark, "customers", "customers", batch_date)

    assert dict(df.dtypes)["customer_id"] == "string"
    assert {row["customer_id"] for row in df.collect()} == {"1", "2"}


@mock_aws
def test_write_silver_round_trips_through_storage(spark):
    ensure_bucket(LakeLayer.SILVER)
    df = spark.createDataFrame([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])

    result = write_silver(df, "customers", "customers", date(2024, 1, 1))

    assert result.row_count == 2
    assert result.silver_uri.startswith("s3://")


@mock_aws
def test_silver_to_spark_df_preserves_types_without_stringifying(spark):
    ensure_bucket(LakeLayer.SILVER)
    batch_date = date(2024, 1, 1)
    df = spark.createDataFrame([{"order_id": 100, "customer_id": 1}])
    write_silver(df, "sales", "orders", batch_date)

    loaded = silver_to_spark_df(spark, "sales", "orders", batch_date)

    assert dict(loaded.dtypes)["order_id"] == "bigint"
    assert loaded.collect()[0]["order_id"] == 100
    assert read_silver("sales", "orders", batch_date)[0]["order_id"] == 100


@mock_aws
def test_silver_to_spark_df_returns_empty_when_no_data(spark):
    ensure_bucket(LakeLayer.SILVER)
    loaded = silver_to_spark_df(spark, "sales", "orders", date(2024, 1, 1))
    assert loaded.count() == 0
