"""Bronze suppliers (weekly CSV catalog) -> Silver supplier_catalog.

Cleaning applied:
- cast supplier_id to int, unit_cost to double, lead_time_days to int
- drop rows missing product_sku (an incomplete catalog line is unusable —
  see sample_data/suppliers/*.csv, which deliberately has one)
- standardize updated_at across the two formats the raw CSV drop contains
  (ISO and US slash-separated); leave null if genuinely missing rather than
  guessing
- dedup on (supplier_id, product_sku), keeping the most recently ingested row
"""

from __future__ import annotations

from datetime import date

from monitoring.logging_config import get_logger
from pyspark.sql import functions as F

from spark.jobs.common import (
    bronze_to_spark_df,
    dedup_latest,
    get_spark_session,
    standardize_date,
    write_silver,
)

logger = get_logger(__name__)

SOURCE = "suppliers"
TABLE = "suppliers"
UPDATED_AT_FORMATS = ["yyyy-MM-dd", "MM/dd/yyyy"]


def clean(df):
    df = df.withColumn("supplier_id", F.col("supplier_id").cast("int"))
    df = df.withColumn("unit_cost", F.col("unit_cost").cast("double"))
    df = df.withColumn("lead_time_days", F.col("lead_time_days").cast("int"))

    df = df.withColumn(
        "product_sku", F.when(F.col("product_sku") == "", None).otherwise(F.col("product_sku"))
    )
    df = df.filter(F.col("product_sku").isNotNull())

    df = standardize_date(df, "updated_at", UPDATED_AT_FORMATS)

    df = dedup_latest(df, key_cols=["supplier_id", "product_sku"], order_col="_ingested_at")
    return df


def run(batch_date: date) -> None:
    spark = get_spark_session("silver-suppliers")
    raw = bronze_to_spark_df(spark, SOURCE, TABLE, batch_date)
    cleaned = clean(raw)
    write_silver(cleaned, SOURCE, TABLE, batch_date)


if __name__ == "__main__":
    import sys

    run(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today())
