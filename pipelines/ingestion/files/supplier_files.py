"""Ingests supplier CSV file drops (weekly). Reads every file matching the
configured glob pattern in the drop directory — a real SFTP/file-share drop
in production, `sample_data/suppliers/` in local dev."""

from __future__ import annotations

import csv
from collections.abc import Iterator
from pathlib import Path

from monitoring.logging_config import get_logger

from pipelines.ingestion.base import BaseIngestion

logger = get_logger(__name__)


class SupplierFileIngestion(BaseIngestion):
    def extract(self) -> Iterator[dict]:
        directory = Path(self.config.directory)
        pattern = self.config.file_pattern or "*.csv"
        files = sorted(directory.glob(pattern))
        if not files:
            logger.warning(
                "ingestion.no_files_found",
                extra={"context": {"directory": str(directory), "pattern": pattern}},
            )
        for file_path in files:
            yield from self._read_csv(file_path)

    def _read_csv(self, file_path: Path) -> Iterator[dict]:
        with file_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["_source_file"] = file_path.name
                yield row
