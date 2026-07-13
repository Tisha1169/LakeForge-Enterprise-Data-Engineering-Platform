"""Shared ingestion lifecycle: connect -> extract -> land -> report.

Concrete ingestion classes (api/db/file) only implement `extract()`, which
yields raw records as dicts. Everything else — retry policy, NDJSON
serialization, writing to the landing zone, timing, logging — lives here
once.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import UTC, date, datetime

from config.sources import SourceConfig
from metadata.client import track_run
from monitoring.logging_config import get_logger
from sqlalchemy import Engine

from pipelines.storage import LakeLayer, ObjectKey, put_bytes

logger = get_logger(__name__)


@dataclass
class IngestionResult:
    source_name: str
    status: str  # "success" | "failed"
    row_count: int
    started_at: datetime
    finished_at: datetime
    landing_uri: str | None = None
    error: str | None = None

    @property
    def duration_seconds(self) -> float:
        return (self.finished_at - self.started_at).total_seconds()


class BaseIngestion(ABC):
    """One subclass per source. `config.name` must match a file under
    config/sources/ and is used as the landing-zone `source` path segment.
    """

    def __init__(
        self,
        config: SourceConfig,
        batch_date: date | None = None,
        metadata_engine: Engine | None = None,
    ):
        self.config = config
        self.batch_date = batch_date or date.today()
        # Opt-in: passing None (the default) skips metadata tracking
        # entirely, so ingestion is fully usable/testable without a
        # metadata database — Airflow tasks pass a real engine explicitly.
        self.metadata_engine = metadata_engine

    @abstractmethod
    def extract(self) -> Iterator[dict]:
        """Yield raw records. Implementations own their own retry logic for
        the specific failure modes of their transport (HTTP, DB, filesystem)."""
        raise NotImplementedError

    @property
    def table_name(self) -> str:
        return self.config.table or self.config.name

    def run(self) -> IngestionResult:
        started_at = datetime.now(UTC)
        logger.info(
            "ingestion.start",
            extra={"context": {"source": self.config.name, "batch_date": str(self.batch_date)}},
        )
        tracker = (
            track_run(
                self.metadata_engine,
                pipeline_name=f"{self.config.name}_ingestion",
                layer="ingestion",
                source_name=self.config.name,
                table_name=self.table_name,
                batch_date=self.batch_date,
            )
            if self.metadata_engine is not None
            else nullcontext(None)
        )
        try:
            with tracker as run_handle:
                records = list(self.extract())
                landing_uri = self._land(records)
                if run_handle is not None:
                    run_handle.row_count = len(records)
                result = IngestionResult(
                    source_name=self.config.name,
                    status="success",
                    row_count=len(records),
                    started_at=started_at,
                    finished_at=datetime.now(UTC),
                    landing_uri=landing_uri,
                )
                logger.info(
                    "ingestion.success",
                    extra={
                        "context": {
                            "source": self.config.name,
                            "row_count": result.row_count,
                            "duration_seconds": result.duration_seconds,
                            "landing_uri": landing_uri,
                        }
                    },
                )
                return result
        except Exception as exc:  # noqa: BLE001 - deliberately broad: any failure is reported, not swallowed
            logger.error(
                "ingestion.failed",
                extra={"context": {"source": self.config.name, "error": str(exc)}},
                exc_info=True,
            )
            raise

    def _land(self, records: list[dict]) -> str:
        ndjson = "\n".join(json.dumps(record, default=str) for record in records).encode("utf-8")
        key = ObjectKey(
            source=self.config.name,
            table=self.table_name,
            filename=f"{self.config.name}_{self.batch_date.isoformat()}.ndjson",
            batch_date=self.batch_date,
        )
        return put_bytes(LakeLayer.LANDING, key, ndjson, content_type="application/x-ndjson")
