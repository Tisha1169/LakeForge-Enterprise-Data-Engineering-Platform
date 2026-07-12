from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from config.sources import SourceConfig
from pipelines.ingestion.db.table_extract import DatabaseTableIngestion


def _config() -> SourceConfig:
    return SourceConfig(name="sales", source_type="db", schema_name="sales", table="orders")


class _FakeResult:
    def __init__(self, columns, rows):
        self._columns = columns
        self._rows = rows

    def keys(self):
        return self._columns

    def __iter__(self):
        return iter(self._rows)


def test_extract_yields_rows_as_dicts():
    columns = ["order_id", "order_status"]
    rows = [(1, "completed"), (2, "pending")]

    fake_conn = MagicMock()
    fake_conn.execute.return_value = _FakeResult(columns, rows)

    fake_engine = MagicMock()

    @contextmanager
    def fake_connect():
        yield fake_conn

    fake_engine.connect = fake_connect

    ingestion = DatabaseTableIngestion(_config())
    with patch.object(ingestion, "_connect", return_value=fake_engine):
        records = list(ingestion.extract())

    assert records == [
        {"order_id": 1, "order_status": "completed"},
        {"order_id": 2, "order_status": "pending"},
    ]
