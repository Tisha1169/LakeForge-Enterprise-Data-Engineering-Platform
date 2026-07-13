"""Shared PySpark session setup, Bronze->Spark bridging, and reusable
cleaning transforms used by every Silver job.

I/O deliberately goes through `pipelines.storage`/`pipelines.bronze` (the
already-tested boto3 boundary) rather than Spark's native S3A connector —
see spark/README.md for why. Spark itself does all the real transformation
work: window functions, casting, joins.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
from monitoring.logging_config import get_logger
from pipelines.bronze.reader import read_bronze
from pipelines.storage import LakeLayer, ObjectKey, put_bytes
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logger = get_logger(__name__)


def get_spark_session(app_name: str) -> SparkSession:
    master = os.environ.get("SPARK_MASTER_URL", "local[*]")
    return SparkSession.builder.appName(app_name).master(master).getOrCreate()


def _stringify(value: object) -> str | None:
    """Bronze may hand back a field that's an int in one row and a string in
    another (raw, unvalidated source data). Spark's own schema inference has
    the same failure mode PyArrow did in Phase 8 — so every Bronze field
    becomes a string on the way into Spark, and each Silver job does its own
    explicit, safe `.cast(...)` afterward."""
    if value is None:
        return None
    return str(value)


def bronze_to_spark_df(spark: SparkSession, source: str, table: str, batch_date: date) -> DataFrame:
    records = read_bronze(source, table, batch_date)
    stringified = [{k: _stringify(v) for k, v in record.items()} for record in records]
    if not stringified:
        logger.warning(
            "silver.no_bronze_records", extra={"context": {"source": source, "table": table}}
        )
        return spark.createDataFrame([], schema="_empty STRING")
    return spark.createDataFrame(stringified)


@dataclass
class SilverWriteResult:
    source: str
    table: str
    batch_date: date
    row_count: int
    silver_uri: str


def write_silver(df: DataFrame, source: str, table: str, batch_date: date) -> SilverWriteResult:
    """Collects the (already-cleaned, portfolio-scale) DataFrame to the
    driver and writes it as a single Parquet partition via `pipelines.storage`.
    Fine at this data volume; a native distributed Spark writer is the
    natural upgrade path once real S3A connectivity is introduced (Phase 12).

    Builds the Arrow table directly from `df.collect()` rather than via
    `DataFrame.toPandas()`: PySpark's `toPandas()` path imports `distutils`,
    which was removed in Python 3.12+, breaking it outright on this
    platform's Python 3.13. Going through Arrow directly also sidesteps an
    unnecessary pandas round-trip — every column here already has a single,
    consistent type (Spark DataFrames guarantee that), so unlike Bronze's
    raw JSON payload, Arrow can build the table with no schema conflicts.
    """
    rows = [row.asDict() for row in df.collect()]
    arrow_table = (
        pa.Table.from_pylist(rows) if rows else pa.Table.from_pylist([], schema=pa.schema([]))
    )
    buffer = pa.BufferOutputStream()
    pq.write_table(arrow_table, buffer, compression="snappy")

    key = ObjectKey(source=source, table=table, filename="part-0.parquet", batch_date=batch_date)
    silver_uri = put_bytes(
        LakeLayer.SILVER,
        key,
        buffer.getvalue().to_pybytes(),
        content_type="application/octet-stream",
    )

    result = SilverWriteResult(
        source=source,
        table=table,
        batch_date=batch_date,
        row_count=len(rows),
        silver_uri=silver_uri,
    )
    logger.info(
        "silver.write",
        extra={
            "context": {
                "source": source,
                "table": table,
                "batch_date": batch_date.isoformat(),
                "row_count": result.row_count,
                "silver_uri": silver_uri,
            }
        },
    )
    return result


def dedup_latest(df: DataFrame, key_cols: list[str], order_col: str) -> DataFrame:
    """Keeps one row per `key_cols`, the most recent by `order_col`."""
    window = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def standardize_date(
    df: DataFrame, col_name: str, formats: list[str], out_col: str | None = None
) -> DataFrame:
    """Tries each format in order (first match wins) via `coalesce`, so a
    column with genuinely inconsistent date formats across rows (real source
    data, see sample_data/) still parses instead of nulling out entirely.

    Uses `try_to_date` rather than `to_date`: under Spark's ANSI SQL mode
    (the default since Spark 4.0 — this project's docker/spark/Dockerfile
    pins 3.5, where ANSI defaults off, but the code shouldn't silently break
    if that ever changes), `to_date` raises on a non-matching format instead
    of returning NULL, which breaks this coalesce-first-match pattern
    entirely. `try_to_date` returns NULL on a mismatch regardless of ANSI
    mode, which is what "try the next format" actually requires."""
    out_col = out_col or col_name
    parsed = [F.try_to_date(F.col(col_name), fmt) for fmt in formats]
    return df.withColumn(out_col, F.coalesce(*parsed))


def trim_and_lower(df: DataFrame, col_name: str) -> DataFrame:
    return df.withColumn(col_name, F.lower(F.trim(F.col(col_name))))
