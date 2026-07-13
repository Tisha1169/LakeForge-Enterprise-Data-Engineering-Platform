"""Reads a Bronze Parquet partition back into memory. Used by Silver jobs
(Phase 9) and by tests/ops tooling that need to inspect what landed in
Bronze without going through Spark.

Each Bronze row is `{"payload": "<json string>", "_ingested_at": ..., ...}`
(see `writer.py` for why) — `read_bronze` parses `payload` back into a dict
and merges in the technical columns, so callers get the original raw record
(exact types preserved) plus audit metadata in one flat dict.
"""

from __future__ import annotations

import io
import json
from datetime import date

import pyarrow.parquet as pq

from pipelines.storage import LakeLayer, ObjectKey, get_bytes, object_exists


def read_bronze(source: str, table: str, batch_date: date) -> list[dict]:
    """Returns [] rather than raising when the partition doesn't exist —
    a source on a non-daily cadence (e.g. suppliers, `@weekly`) legitimately
    has no Bronze data for most batch_dates, and Silver jobs run daily
    regardless (see airflow/dags/silver_transform_dag.py)."""
    key = ObjectKey(source=source, table=table, filename="part-0.parquet", batch_date=batch_date)
    if not object_exists(LakeLayer.BRONZE, key):
        return []
    raw = get_bytes(LakeLayer.BRONZE, key)
    arrow_table = pq.read_table(io.BytesIO(raw))

    rows = []
    for row in arrow_table.to_pylist():
        record = json.loads(row.pop("payload"))
        record.update(row)
        rows.append(record)
    return rows
