"""Builds every Gold table for a batch_date, in dependency order:
dimensions first (fact_sales needs their surrogate keys), then the fact
table, then aggregates built on top of the fact table.
"""

from __future__ import annotations

from datetime import date

from monitoring.logging_config import get_logger

from pipelines.gold.daily_sales_summary import build_daily_sales_summary
from pipelines.gold.dim_customer import build_dim_customer
from pipelines.gold.dim_date import build_dim_date
from pipelines.gold.dim_product import build_dim_product
from pipelines.gold.dim_store import build_dim_store
from pipelines.gold.fact_sales import build_fact_sales
from pipelines.gold.writer import read_gold_table, write_gold_table

logger = get_logger(__name__)


def run_gold_build(batch_date: date) -> dict[str, int]:
    dim_date_rows = read_gold_table("dim_date") or build_dim_date()
    if not read_gold_table("dim_date"):
        write_gold_table("dim_date", dim_date_rows)

    dim_customer_rows = build_dim_customer(batch_date)
    write_gold_table("dim_customer", dim_customer_rows)

    dim_product_rows = build_dim_product(batch_date)
    write_gold_table("dim_product", dim_product_rows)

    dim_store_rows = build_dim_store(batch_date)
    write_gold_table("dim_store", dim_store_rows)

    fact_sales_rows = build_fact_sales(
        batch_date, dim_customer_rows, dim_product_rows, dim_store_rows, dim_date_rows
    )
    write_gold_table("fact_sales", fact_sales_rows)

    daily_summary_rows = build_daily_sales_summary(fact_sales_rows, dim_store_rows)
    write_gold_table("daily_sales_summary", daily_summary_rows)

    row_counts = {
        "dim_date": len(dim_date_rows),
        "dim_customer": len(dim_customer_rows),
        "dim_product": len(dim_product_rows),
        "dim_store": len(dim_store_rows),
        "fact_sales": len(fact_sales_rows),
        "daily_sales_summary": len(daily_summary_rows),
    }
    logger.info(
        "gold.build_complete",
        extra={"context": {"batch_date": batch_date.isoformat(), **row_counts}},
    )
    return row_counts


if __name__ == "__main__":
    import sys

    run_gold_build(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today())
