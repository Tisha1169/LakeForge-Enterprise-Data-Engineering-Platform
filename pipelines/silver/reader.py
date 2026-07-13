"""Reads a Silver Parquet partition back into memory. Used by Gold builders
(Phase 10) — symmetric to `pipelines.bronze.reader`."""

from __future__ import annotations

import io
from datetime import date

import pyarrow.parquet as pq

from pipelines.storage import LakeLayer, ObjectKey, get_bytes, object_exists


def read_silver(source: str, table: str, batch_date: date) -> list[dict]:
    key = ObjectKey(source=source, table=table, filename="part-0.parquet", batch_date=batch_date)
    if not object_exists(LakeLayer.SILVER, key):
        return []
    raw = get_bytes(LakeLayer.SILVER, key)
    return pq.read_table(io.BytesIO(raw)).to_pylist()
