"""Bronze sales_stores.stores -> Silver stores.

Cleaning applied:
- cast store_id to int, opened_date to date
- drop rows missing store_id
- dedup on store_id, keeping the most recently ingested record
"""

from __future__ import annotations

from datetime import date

from monitoring.logging_config import get_logger
from pyspark.sql import functions as F

from spark.jobs.common import bronze_to_spark_df, dedup_latest, get_spark_session, write_silver

logger = get_logger(__name__)

SOURCE = "sales_stores"
TABLE = "stores"


def clean(df):
    df = df.withColumn("store_id", F.col("store_id").cast("int"))
    df = df.withColumn("opened_date", F.to_date(F.col("opened_date")))
    df = df.filter(F.col("store_id").isNotNull())

    df = dedup_latest(df, key_cols=["store_id"], order_col="_ingested_at")
    return df


def run(batch_date: date) -> None:
    spark = get_spark_session("silver-stores")
    raw = bronze_to_spark_df(spark, SOURCE, TABLE, batch_date)
    cleaned = clean(raw)
    write_silver(cleaned, SOURCE, TABLE, batch_date)


if __name__ == "__main__":
    import sys

    run(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today())
