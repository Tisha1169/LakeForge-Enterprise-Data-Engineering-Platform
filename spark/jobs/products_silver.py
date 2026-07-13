"""Bronze sales_products.products -> Silver products.

Cleaning applied:
- cast product_id to int, unit_price to double
- drop rows missing product_id or sku (the product master's natural keys)
- dedup on product_id, keeping the most recently ingested record
"""

from __future__ import annotations

from datetime import date

from monitoring.logging_config import get_logger
from pyspark.sql import functions as F

from spark.jobs.common import bronze_to_spark_df, dedup_latest, get_spark_session, write_silver

logger = get_logger(__name__)

SOURCE = "sales_products"
TABLE = "products"


def clean(df):
    df = df.withColumn("product_id", F.col("product_id").cast("int"))
    df = df.withColumn("unit_price", F.col("unit_price").cast("double"))

    df = df.withColumn("sku", F.when(F.col("sku") == "", None).otherwise(F.col("sku")))
    df = df.filter(F.col("product_id").isNotNull() & F.col("sku").isNotNull())

    df = dedup_latest(df, key_cols=["product_id"], order_col="_ingested_at")
    return df


def run(batch_date: date) -> None:
    spark = get_spark_session("silver-products")
    raw = bronze_to_spark_df(spark, SOURCE, TABLE, batch_date)
    cleaned = clean(raw)
    write_silver(cleaned, SOURCE, TABLE, batch_date)


if __name__ == "__main__":
    import sys

    run(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today())
