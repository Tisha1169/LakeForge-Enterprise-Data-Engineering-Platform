from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
from pipelines.storage import LakeLayer, ObjectKey, ensure_bucket, put_bytes


def seed_silver(source: str, table: str, batch_date: date, rows: list[dict]) -> None:
    ensure_bucket(LakeLayer.SILVER)
    arrow_table = (
        pa.Table.from_pylist(rows) if rows else pa.Table.from_pylist([], schema=pa.schema([]))
    )
    buffer = pa.BufferOutputStream()
    pq.write_table(arrow_table, buffer)
    key = ObjectKey(source=source, table=table, filename="part-0.parquet", batch_date=batch_date)
    put_bytes(LakeLayer.SILVER, key, buffer.getvalue().to_pybytes())
