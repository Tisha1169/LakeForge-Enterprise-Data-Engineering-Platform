"""Bronze customers -> Silver customers.

Cleaning applied:
- cast customer_id to int, drop rows where it's missing/unparseable
  (customer_id is the grain — an unidentifiable customer isn't a usable row)
- lowercase + trim email; null out empty strings
- standardize signup_date across the three formats the raw API/sample data
  actually contains (ISO, slash-separated, US-style)
- dedup on customer_id, keeping the most recently ingested record
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
    trim_and_lower,
    write_silver,
)

logger = get_logger(__name__)

SOURCE = "customers"
TABLE = "customers"
SIGNUP_DATE_FORMATS = ["yyyy-MM-dd", "yyyy/MM/dd", "MM-dd-yyyy"]


def clean(df):
    df = df.withColumn("customer_id", F.col("customer_id").cast("int"))
    df = df.filter(F.col("customer_id").isNotNull())

    df = trim_and_lower(df, "email")
    df = df.withColumn("email", F.when(F.col("email") == "", None).otherwise(F.col("email")))

    df = standardize_date(df, "signup_date", SIGNUP_DATE_FORMATS)

    df = df.withColumn("first_name", F.trim(F.col("first_name")))
    df = df.withColumn("last_name", F.trim(F.col("last_name")))

    df = dedup_latest(df, key_cols=["customer_id"], order_col="_ingested_at")
    return df


def run(batch_date: date) -> None:
    spark = get_spark_session("silver-customers")
    raw = bronze_to_spark_df(spark, SOURCE, TABLE, batch_date)
    cleaned = clean(raw)
    write_silver(cleaned, SOURCE, TABLE, batch_date)


if __name__ == "__main__":
    import sys

    run(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today())
