import pytest


@pytest.fixture(autouse=True)
def _configure_settings(monkeypatch):
    import pipelines.storage as storage_module

    monkeypatch.setattr(storage_module.settings, "minio_endpoint", "s3.amazonaws.com")
    monkeypatch.setattr(storage_module.settings, "minio_access_key", "testing")
    monkeypatch.setattr(storage_module.settings, "minio_secret_key", "testing")
    monkeypatch.setattr(storage_module.settings, "minio_secure", True)
    monkeypatch.setattr(storage_module.settings, "minio_bucket_silver", "test-silver")
    monkeypatch.setattr(storage_module.settings, "minio_bucket_gold", "test-gold")
