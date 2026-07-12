"""Generic operational-DB table extraction (used for both Sales and
Inventory sources — the logic is identical, only the config differs)."""

from __future__ import annotations

from collections.abc import Iterator

from config.settings import settings
from monitoring.logging_config import get_logger
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from pipelines.ingestion.base import BaseIngestion

logger = get_logger(__name__)


class DatabaseTableIngestion(BaseIngestion):
    """Extracts an entire table from the source Postgres as raw rows.

    Full-table extraction today; incremental (`updated_ts > last_run`)
    extraction is introduced as part of the ETL/incremental-load work in
    later phases once metadata run-tracking (Phase 15) exists to record
    watermarks.
    """

    def extract(self) -> Iterator[dict]:
        engine = self._connect()
        schema = self.config.schema_name
        table = self.config.table
        query = text(f"SELECT * FROM {schema}.{table}")  # noqa: S608 - schema/table from trusted YAML config, not user input
        with engine.connect() as conn:
            result = conn.execute(query)
            columns = result.keys()
            for row in result:
                yield dict(zip(columns, row, strict=True))

    @retry(
        retry=retry_if_exception_type(OperationalError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=20),
        reraise=True,
    )
    def _connect(self):
        engine = create_engine(settings.source_db_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
