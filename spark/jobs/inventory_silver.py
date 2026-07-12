"""Bronze inventory.stock_snapshots -> Silver stock_snapshots.

Cleaning applied:
- cast snapshot_id/store_id/product_id/quantity_on_hand/reorder_point/
  supplier_id to their proper numeric types
- cast snapshot_date to date
- drop rows missing the grain (store_id, product_id, snapshot_date)
- dedup on the grain, keeping the most recently ingested record (a snapshot
  for the same store/product/date should never legitimately repeat, but
  re-runs of a batch could otherwise duplicate it)
"""

from __future__ import annotations

from datetime import date

from monitoring.logging_config import get_logger
from pyspark.sql import functions as F

from spark.jobs.common import bronze_to_spark_df, dedup_latest, get_spark_session, write_silver

logger = get_logger(__name__)

SOURCE = "inventory"
TABLE = "stock_snapshots"


def clean(df):
    df = df.withColumn("snapshot_id", F.col("snapshot_id").cast("long"))
    df = df.withColumn("store_id", F.col("store_id").cast("int"))
    df = df.withColumn("product_id", F.col("product_id").cast("int"))
    df = df.withColumn("quantity_on_hand", F.col("quantity_on_hand").cast("int"))
    df = df.withColumn("reorder_point", F.col("reorder_point").cast("int"))
    df = df.withColumn("supplier_id", F.col("supplier_id").cast("int"))
    df = df.withColumn("snapshot_date", F.to_date(F.col("snapshot_date")))

    df = df.filter(
        F.col("store_id").isNotNull()
        & F.col("product_id").isNotNull()
        & F.col("snapshot_date").isNotNull()
    )

    df = dedup_latest(
        df, key_cols=["store_id", "product_id", "snapshot_date"], order_col="_ingested_at"
    )
    return df


def run(batch_date: date) -> None:
    spark = get_spark_session("silver-inventory")
    raw = bronze_to_spark_df(spark, SOURCE, TABLE, batch_date)
    cleaned = clean(raw)
    write_silver(cleaned, SOURCE, TABLE, batch_date)


if __name__ == "__main__":
    import sys

    run(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today())
