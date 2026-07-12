"""Bronze sales.orders -> Silver orders.

Cleaning applied:
- cast order_id/customer_id/store_id to int, drop rows missing order_id
- cast order_ts/updated_ts to timestamp
- validate order_status against the known set; unknown values become
  "unknown" rather than silently passing through (a real business rule, not
  just type safety)
- dedup on order_id keeping the most recently *updated* row — orders are
  mutable in the source system (status changes), so "latest updated_ts wins"
  is the correct dedup key here, not "latest ingested"
"""

from __future__ import annotations

from datetime import date

from monitoring.logging_config import get_logger
from pyspark.sql import functions as F

from spark.jobs.common import bronze_to_spark_df, dedup_latest, get_spark_session, write_silver

logger = get_logger(__name__)

SOURCE = "sales"
TABLE = "orders"
KNOWN_STATUSES = {"pending", "completed", "cancelled", "refunded"}


def clean(df):
    df = df.withColumn("order_id", F.col("order_id").cast("int"))
    df = df.withColumn("customer_id", F.col("customer_id").cast("int"))
    df = df.withColumn("store_id", F.col("store_id").cast("int"))
    df = df.filter(F.col("order_id").isNotNull())

    df = df.withColumn("order_ts", F.to_timestamp(F.col("order_ts")))
    df = df.withColumn("updated_ts", F.to_timestamp(F.col("updated_ts")))

    df = df.withColumn(
        "order_status",
        F.when(F.col("order_status").isin(list(KNOWN_STATUSES)), F.col("order_status")).otherwise(
            F.lit("unknown")
        ),
    )

    df = dedup_latest(df, key_cols=["order_id"], order_col="updated_ts")
    return df


def run(batch_date: date) -> None:
    spark = get_spark_session("silver-sales-orders")
    raw = bronze_to_spark_df(spark, SOURCE, TABLE, batch_date)
    cleaned = clean(raw)
    write_silver(cleaned, SOURCE, TABLE, batch_date)


if __name__ == "__main__":
    import sys

    run(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today())
