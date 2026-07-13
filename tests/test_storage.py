from datetime import date

import pytest
from moto import mock_aws
from pipelines.storage import (
    LakeLayer,
    ObjectKey,
    ensure_bucket,
    get_bytes,
    list_objects,
    object_exists,
    put_bytes,
)


@pytest.fixture(autouse=True)
def _configure_settings(monkeypatch):
    import pipelines.storage as storage_module

    monkeypatch.setattr(storage_module.settings, "minio_endpoint", "s3.amazonaws.com")
    monkeypatch.setattr(storage_module.settings, "minio_access_key", "testing")
    monkeypatch.setattr(storage_module.settings, "minio_secret_key", "testing")
    monkeypatch.setattr(storage_module.settings, "minio_secure", True)
    monkeypatch.setattr(storage_module.settings, "minio_bucket_bronze", "test-bronze")


def test_object_key_uses_batch_date_when_partition_not_given():
    key = ObjectKey(
        source="inventory",
        table="stock_snapshots",
        filename="data.parquet",
        batch_date=date(2024, 1, 15),
    )
    assert str(key) == "inventory/stock_snapshots/batch_date=2024-01-15/data.parquet"


def test_object_key_prefers_explicit_partition():
    key = ObjectKey(
        source="sales",
        table="orders",
        filename="data.parquet",
        partition="region=west",
        batch_date=date(2024, 1, 15),
    )
    assert str(key) == "sales/orders/region=west/data.parquet"


@mock_aws
def test_put_get_roundtrip():
    ensure_bucket(LakeLayer.BRONZE)
    key = ObjectKey(
        source="sales",
        table="orders",
        filename="part-0.parquet",
        partition="batch_date=2024-01-01",
    )
    put_bytes(LakeLayer.BRONZE, key, b"hello world")

    assert object_exists(LakeLayer.BRONZE, key)
    assert get_bytes(LakeLayer.BRONZE, key) == b"hello world"
    assert str(key) in list_objects(LakeLayer.BRONZE, prefix="sales/orders")


@mock_aws
def test_object_exists_false_when_missing():
    ensure_bucket(LakeLayer.BRONZE)
    key = ObjectKey(source="sales", table="orders", filename="missing.parquet")
    assert not object_exists(LakeLayer.BRONZE, key)


@mock_aws
def test_object_exists_false_when_bucket_itself_does_not_exist_yet():
    key = ObjectKey(source="gold", table="dim_customer", filename="part-0.parquet")
    assert not object_exists(LakeLayer.BRONZE, key)
