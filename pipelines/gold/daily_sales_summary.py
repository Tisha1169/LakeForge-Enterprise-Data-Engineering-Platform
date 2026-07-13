"""daily_sales_summary — business KPI aggregate over fact_sales, grain:
(date_key, store_id). Excludes cancelled/refunded orders from revenue,
since counting those would overstate actual sales performance."""

from __future__ import annotations

import duckdb

from pipelines.gold.common import fetch_as_dicts, register_rows
from pipelines.gold.schemas import DIM_STORE_SCHEMA


def build_daily_sales_summary(
    fact_sales_rows: list[dict], dim_store_rows: list[dict]
) -> list[dict]:
    if not fact_sales_rows:
        return []

    con = duckdb.connect()
    register_rows(con, "fact_sales", fact_sales_rows)
    register_rows(con, "dim_store", dim_store_rows, empty_schema=DIM_STORE_SCHEMA)

    return fetch_as_dicts(
        con,
        """
        SELECT
            f.date_key AS date_key,
            ds.store_id AS store_id,
            ds.store_name AS store_name,
            COUNT(DISTINCT f.order_id) AS total_orders,
            SUM(f.quantity) AS total_quantity,
            ROUND(SUM(f.extended_amount), 2) AS total_revenue,
            ROUND(SUM(f.extended_amount) / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) AS avg_order_value
        FROM fact_sales f
        LEFT JOIN dim_store ds ON ds.store_sk = f.store_sk
        WHERE f.order_status NOT IN ('cancelled', 'refunded')
        GROUP BY f.date_key, ds.store_id, ds.store_name
        """,
    )
