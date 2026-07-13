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


def seed_bronze(source: str, table: str, batch_date: date, rows: list[dict]) -> None:
    """Writes rows through the real Bronze writer (payload-column format),
    not a raw Parquet write — Bronze validation needs to read the actual
    on-disk shape (see pipelines/bronze/reader.py)."""
    import json

    from pipelines.bronze.writer import write_bronze
    from pipelines.storage import LakeLayer as _LakeLayer

    ensure_bucket(_LakeLayer.LANDING)
    ndjson = "\n".join(json.dumps(r, default=str) for r in rows).encode("utf-8")
    filename = f"{source}_{batch_date.isoformat()}.ndjson"
    key = ObjectKey(source=source, table=table, filename=filename, batch_date=batch_date)
    put_bytes(_LakeLayer.LANDING, key, ndjson)
    ensure_bucket(LakeLayer.BRONZE)
    write_bronze(source, table, batch_date, filename)
