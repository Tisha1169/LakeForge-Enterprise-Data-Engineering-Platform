"""Read/write helpers for Gold tables.

Unlike Bronze/Silver, Gold tables are NOT batch_date-partitioned — each one
is the full current star-schema table (dimensions are merged/upserted in
place per run; `fact_sales` is a full rebuild at this data volume). A real
high-volume deployment would partition `fact_sales` by date; noted as a
scaling follow-up, not needed at portfolio scale.
"""

from __future__ import annotations

import io

import pyarrow as pa
import pyarrow.parquet as pq
from monitoring.logging_config import get_logger

from pipelines.storage import LakeLayer, ObjectKey, get_bytes, object_exists, put_bytes

logger = get_logger(__name__)


def _key(table_name: str) -> ObjectKey:
    return ObjectKey(source="gold", table=table_name, filename="part-0.parquet")


def write_gold_table(table_name: str, rows: list[dict]) -> str:
    arrow_table = (
        pa.Table.from_pylist(rows) if rows else pa.Table.from_pylist([], schema=pa.schema([]))
    )
    buffer = pa.BufferOutputStream()
    pq.write_table(arrow_table, buffer, compression="snappy")

    uri = put_bytes(
        LakeLayer.GOLD,
        _key(table_name),
        buffer.getvalue().to_pybytes(),
        content_type="application/octet-stream",
    )
    logger.info(
        "gold.write",
        extra={"context": {"table": table_name, "row_count": len(rows), "gold_uri": uri}},
    )
    return uri


def read_gold_table(table_name: str) -> list[dict]:
    key = _key(table_name)
    if not object_exists(LakeLayer.GOLD, key):
        return []
    raw = get_bytes(LakeLayer.GOLD, key)
    return pq.read_table(io.BytesIO(raw)).to_pylist()
