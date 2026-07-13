import json
from datetime import date

import pytest
from moto import mock_aws
from pipelines.bronze.reader import read_bronze
from pipelines.bronze.writer import write_bronze
from pipelines.storage import LakeLayer, ObjectKey, ensure_bucket, put_bytes


@pytest.fixture(autouse=True)
def _configure_settings(monkeypatch):
    import pipelines.storage as storage_module

    monkeypatch.setattr(storage_module.settings, "minio_endpoint", "s3.amazonaws.com")
    monkeypatch.setattr(storage_module.settings, "minio_access_key", "testing")
    monkeypatch.setattr(storage_module.settings, "minio_secret_key", "testing")
    monkeypatch.setattr(storage_module.settings, "minio_secure", True)
    monkeypatch.setattr(storage_module.settings, "minio_bucket_landing", "test-landing")
    monkeypatch.setattr(storage_module.settings, "minio_bucket_bronze", "test-bronze")


def _seed_landing(
    source: str, table: str, batch_date: date, filename: str, records: list[dict]
) -> None:
    ensure_bucket(LakeLayer.LANDING)
    ndjson = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    key = ObjectKey(source=source, table=table, filename=filename, batch_date=batch_date)
    put_bytes(LakeLayer.LANDING, key, ndjson)


@mock_aws
def test_write_bronze_preserves_raw_values_and_adds_technical_columns():
    ensure_bucket(LakeLayer.BRONZE)
    batch_date = date(2024, 1, 1)
    records = [
        {"customer_id": 1, "email": "a@example.com"},
        {"customer_id": "2", "email": None},  # deliberately "wrong" type — Bronze must not fix it
    ]
    _seed_landing("customers", "customers", batch_date, "customers_2024-01-01.ndjson", records)

    result = write_bronze("customers", "customers", batch_date, "customers_2024-01-01.ndjson")

    assert result.row_count == 2
    written = read_bronze("customers", "customers", batch_date)
    assert len(written) == 2
    assert written[0]["customer_id"] == 1
    assert written[1]["customer_id"] == "2"  # untouched, still a string
    assert all(
        "_ingested_at" in row and "_source_file" in row and "_batch_date" in row for row in written
    )
    assert written[0]["_batch_date"] == "2024-01-01"


@mock_aws
def test_write_bronze_is_idempotent_overwrites_same_partition():
    ensure_bucket(LakeLayer.BRONZE)
    batch_date = date(2024, 1, 2)
    _seed_landing(
        "customers", "customers", batch_date, "customers_2024-01-02.ndjson", [{"customer_id": 1}]
    )
    write_bronze("customers", "customers", batch_date, "customers_2024-01-02.ndjson")

    # Re-run with different data for the same partition.
    _seed_landing(
        "customers",
        "customers",
        batch_date,
        "customers_2024-01-02.ndjson",
        [{"customer_id": 1}, {"customer_id": 2}],
    )
    result = write_bronze("customers", "customers", batch_date, "customers_2024-01-02.ndjson")

    assert result.row_count == 2
    assert len(read_bronze("customers", "customers", batch_date)) == 2


@mock_aws
def test_read_bronze_returns_empty_list_when_partition_does_not_exist():
    ensure_bucket(LakeLayer.BRONZE)
    # A non-daily source (e.g. suppliers, @weekly) has no Bronze data most days.
    assert read_bronze("suppliers", "suppliers", date(2024, 1, 2)) == []
