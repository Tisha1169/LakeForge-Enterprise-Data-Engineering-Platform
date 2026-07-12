import json
from collections.abc import Iterator
from datetime import date

import pytest
from config.sources import SourceConfig
from moto import mock_aws
from pipelines.ingestion.base import BaseIngestion
from pipelines.storage import LakeLayer, ensure_bucket, get_bytes


class _StubIngestion(BaseIngestion):
    def __init__(self, config, batch_date=None, records=None, fail=False):
        super().__init__(config, batch_date)
        self._records = records or []
        self._fail = fail

    def extract(self) -> Iterator[dict]:
        if self._fail:
            raise RuntimeError("boom")
        yield from self._records


@pytest.fixture(autouse=True)
def _configure_settings(monkeypatch):
    import pipelines.storage as storage_module

    monkeypatch.setattr(storage_module.settings, "minio_endpoint", "s3.amazonaws.com")
    monkeypatch.setattr(storage_module.settings, "minio_access_key", "testing")
    monkeypatch.setattr(storage_module.settings, "minio_secret_key", "testing")
    monkeypatch.setattr(storage_module.settings, "minio_secure", True)
    monkeypatch.setattr(storage_module.settings, "minio_bucket_landing", "test-landing")


@mock_aws
def test_run_lands_ndjson_and_returns_success_result():
    ensure_bucket(LakeLayer.LANDING)
    config = SourceConfig(name="customers", source_type="api")
    records = [
        {"customer_id": 1, "email": "a@example.com"},
        {"customer_id": 2, "email": "b@example.com"},
    ]
    ingestion = _StubIngestion(config, batch_date=date(2024, 1, 1), records=records)

    result = ingestion.run()

    assert result.status == "success"
    assert result.row_count == 2
    assert result.landing_uri is not None

    key = "customers/customers/batch_date=2024-01-01/customers_2024-01-01.ndjson"
    raw = get_bytes(LakeLayer.LANDING, key)
    lines = raw.decode("utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0]) == records[0]


@mock_aws
def test_run_raises_and_reports_failure():
    config = SourceConfig(name="customers", source_type="api")
    ingestion = _StubIngestion(config, fail=True)

    with pytest.raises(RuntimeError, match="boom"):
        ingestion.run()
