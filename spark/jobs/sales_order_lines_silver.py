"""Bronze sales.order_lines -> Silver order_lines.

Cleaning applied:
- cast order_line_id/order_id/product_id to int, quantity to int,
  unit_price/discount_pct to double
- drop rows missing order_line_id or order_id (line-item grain requires both)
- repartition by order_line_id before deduping — dedup_latest's window
  function shuffles on that key regardless, so partitioning by it first
  means Spark plans one shuffle instead of two
- dedup on order_line_id (line items aren't mutated in place in this source,
  but re-ingestion could still produce duplicates across batches)
- BROADCAST JOIN against Silver `orders` (left_semi — filters, adds no
  columns): drops order lines whose order_id doesn't exist in `orders` at
  all — a real data-quality problem (an orphaned line item), not just a
  missing-value check. `orders` (one row per order) is far smaller than
  `order_lines` (multiple rows per order), so broadcasting it to every
  executor avoids shuffling the larger table across the network — the
  standard fact/dimension-size broadcast join pattern. This makes
  sales_order_lines_silver depend on sales_orders_silver having already run
  for the same batch_date (see airflow/dags/silver_transform_dag.py).
"""

from __future__ import annotations

from datetime import date

from monitoring.logging_config import get_logger
from pyspark.sql import functions as F

from spark.jobs.common import (
    bronze_to_spark_df,
    dedup_latest,
    get_spark_session,
    silver_to_spark_df,
    write_silver,
)

logger = get_logger(__name__)

SOURCE = "sales_order_lines"
TABLE = "order_lines"


def clean(df, valid_orders_df):
    df = df.withColumn("order_line_id", F.col("order_line_id").cast("int"))
    df = df.withColumn("order_id", F.col("order_id").cast("int"))
    df = df.withColumn("product_id", F.col("product_id").cast("int"))
    df = df.withColumn("quantity", F.col("quantity").cast("int"))
    df = df.withColumn("unit_price", F.col("unit_price").cast("double"))
    df = df.withColumn("discount_pct", F.col("discount_pct").cast("double"))

    df = df.filter(F.col("order_line_id").isNotNull() & F.col("order_id").isNotNull())

    df = df.repartition("order_line_id")
    df = dedup_latest(df, key_cols=["order_line_id"], order_col="_ingested_at")

    df = df.join(F.broadcast(valid_orders_df.select("order_id")), on="order_id", how="left_semi")
    return df


def run(batch_date: date) -> None:
    spark = get_spark_session("silver-sales-order-lines")
    raw = bronze_to_spark_df(spark, SOURCE, TABLE, batch_date)
    valid_orders = silver_to_spark_df(spark, "sales", "orders", batch_date)
    cleaned = clean(raw, valid_orders)
    write_silver(cleaned, SOURCE, TABLE, batch_date)


if __name__ == "__main__":
    import sys

    run(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today())
