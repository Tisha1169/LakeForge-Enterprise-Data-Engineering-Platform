"""Landing (NDJSON) -> Bronze (partitioned Parquet), with zero transformation.

Bronze stores each raw record as a JSON string in a `payload` column, plus a
few typed technical columns for audit/lineage — it does NOT flatten fields
into typed Arrow columns. That's deliberate: raw, unvalidated source data can
have the same field appear as an int in one row and a string in another
(schema drift), which Arrow's columnar type inference cannot represent in a
single column. Storing the payload as schema-on-read JSON means Bronze can
never fail to write regardless of what a source sends — type casting,
cleaning, and validation happen explicitly in Silver (Phase 9), where they
belong.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime

import pyarrow as pa
import pyarrow.parquet as pq
from monitoring.logging_config import get_logger

from pipelines.storage import LakeLayer, ObjectKey, get_bytes, put_bytes

logger = get_logger(__name__)


@dataclass
class BronzeWriteResult:
    source: str
    table: str
    batch_date: date
    row_count: int
    bronze_uri: str


def _read_landing_ndjson(source: str, table: str, batch_date: date, filename: str) -> list[dict]:
    key = ObjectKey(source=source, table=table, filename=filename, batch_date=batch_date)
    raw = get_bytes(LakeLayer.LANDING, key)
    return [json.loads(line) for line in raw.decode("utf-8").splitlines() if line]


def write_bronze(
    source: str,
    table: str,
    batch_date: date,
    landing_filename: str,
) -> BronzeWriteResult:
    """Reads one landing NDJSON file and writes it as a Bronze Parquet
    partition at `{source}/{table}/batch_date=.../part-0.parquet`.

    Re-running for the same (source, table, batch_date) overwrites that
    partition — idempotent, not append-only within a partition.
    """
    records = _read_landing_ndjson(source, table, batch_date, landing_filename)

    ingested_at = datetime.now(UTC).isoformat()
    rows = [
        {
            "payload": json.dumps(record, default=str),
            "_ingested_at": ingested_at,
            "_source_file": landing_filename,
            "_batch_date": batch_date.isoformat(),
        }
        for record in records
    ]

    schema = pa.schema(
        [
            ("payload", pa.string()),
            ("_ingested_at", pa.string()),
            ("_source_file", pa.string()),
            ("_batch_date", pa.string()),
        ]
    )
    table_arrow = pa.Table.from_pylist(rows, schema=schema)
    buffer = pa.BufferOutputStream()
    pq.write_table(table_arrow, buffer, compression="snappy")

    key = ObjectKey(
        source=source,
        table=table,
        filename="part-0.parquet",
        batch_date=batch_date,
    )
    bronze_uri = put_bytes(
        LakeLayer.BRONZE,
        key,
        buffer.getvalue().to_pybytes(),
        content_type="application/octet-stream",
    )

    result = BronzeWriteResult(
        source=source,
        table=table,
        batch_date=batch_date,
        row_count=len(records),
        bronze_uri=bronze_uri,
    )
    logger.info(
        "bronze.write",
        extra={
            "context": {
                "source": source,
                "table": table,
                "batch_date": batch_date.isoformat(),
                "row_count": result.row_count,
                "bronze_uri": bronze_uri,
            }
        },
    )
    return result
