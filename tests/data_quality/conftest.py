import pytest


@pytest.fixture(autouse=True)
def _configure_settings(monkeypatch, tmp_path):
    import pipelines.storage as storage_module

    monkeypatch.setattr(storage_module.settings, "minio_endpoint", "s3.amazonaws.com")
    monkeypatch.setattr(storage_module.settings, "minio_access_key", "testing")
    monkeypatch.setattr(storage_module.settings, "minio_secret_key", "testing")
    monkeypatch.setattr(storage_module.settings, "minio_secure", True)
    monkeypatch.setattr(storage_module.settings, "minio_bucket_bronze", "test-bronze")
    monkeypatch.setattr(storage_module.settings, "minio_bucket_silver", "test-silver")

    # Keep GX's generated project state out of the repo's real data_quality/ dir.
    monkeypatch.setenv("DATA_QUALITY_GX_ROOT", str(tmp_path / "gx_root"))
