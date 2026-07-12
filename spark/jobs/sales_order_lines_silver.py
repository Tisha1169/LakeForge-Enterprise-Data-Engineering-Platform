"""Bronze sales.order_lines -> Silver order_lines.

Cleaning applied:
- cast order_line_id/order_id/product_id to int, quantity to int,
  unit_price/discount_pct to double
- drop rows missing order_line_id or order_id (line-item grain requires both)
- dedup on order_line_id (line items aren't mutated in place in this source,
  but re-ingestion could still produce duplicates across batches)
"""

from __future__ import annotations

from datetime import date

from monitoring.logging_config import get_logger
from pyspark.sql import functions as F

from spark.jobs.common import bronze_to_spark_df, dedup_latest, get_spark_session, write_silver

logger = get_logger(__name__)

SOURCE = "sales_order_lines"
TABLE = "order_lines"


def clean(df):
    df = df.withColumn("order_line_id", F.col("order_line_id").cast("int"))
    df = df.withColumn("order_id", F.col("order_id").cast("int"))
    df = df.withColumn("product_id", F.col("product_id").cast("int"))
    df = df.withColumn("quantity", F.col("quantity").cast("int"))
    df = df.withColumn("unit_price", F.col("unit_price").cast("double"))
    df = df.withColumn("discount_pct", F.col("discount_pct").cast("double"))

    df = df.filter(F.col("order_line_id").isNotNull() & F.col("order_id").isNotNull())

    df = dedup_latest(df, key_cols=["order_line_id"], order_col="_ingested_at")
    return df


def run(batch_date: date) -> None:
    spark = get_spark_session("silver-sales-order-lines")
    raw = bronze_to_spark_df(spark, SOURCE, TABLE, batch_date)
    cleaned = clean(raw)
    write_silver(cleaned, SOURCE, TABLE, batch_date)


if __name__ == "__main__":
    import sys

    run(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today())
