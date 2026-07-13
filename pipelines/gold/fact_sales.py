"""fact_sales — grain: one row per order line.

Full rebuild each run (see pipelines/gold/writer.py for why that's fine at
this data volume). Joins:
- dim_product / dim_store: Type 1, current-value lookup by natural key
- dim_customer: Type 2, POINT-IN-TIME lookup — the order's customer_sk is
  whichever dim_customer version was effective on the order date, not
  necessarily today's current version. This is the textbook-correct way to
  join a fact to an SCD2 dimension; joining on "current" customer attributes
  would silently rewrite history every time a customer's loyalty tier changes.
- dim_date: by the order's calendar date
"""

from __future__ import annotations

from datetime import date

import duckdb

from pipelines.gold.common import fetch_as_dicts, register_rows
from pipelines.gold.schemas import (
    DIM_CUSTOMER_SCHEMA,
    DIM_DATE_SCHEMA,
    DIM_PRODUCT_SCHEMA,
    DIM_STORE_SCHEMA,
)
from pipelines.silver.reader import read_silver


def build_fact_sales(
    batch_date: date,
    dim_customer_rows: list[dict],
    dim_product_rows: list[dict],
    dim_store_rows: list[dict],
    dim_date_rows: list[dict],
) -> list[dict]:
    order_lines = read_silver("sales_order_lines", "order_lines", batch_date)
    orders = read_silver("sales", "orders", batch_date)
    if not order_lines or not orders:
        return []

    con = duckdb.connect()
    register_rows(con, "order_lines", order_lines)
    register_rows(con, "orders", orders)
    register_rows(con, "dim_customer", dim_customer_rows, empty_schema=DIM_CUSTOMER_SCHEMA)
    register_rows(con, "dim_product", dim_product_rows, empty_schema=DIM_PRODUCT_SCHEMA)
    register_rows(con, "dim_store", dim_store_rows, empty_schema=DIM_STORE_SCHEMA)
    register_rows(con, "dim_date", dim_date_rows, empty_schema=DIM_DATE_SCHEMA)

    return fetch_as_dicts(
        con,
        """
        SELECT
            ol.order_line_id AS order_line_id,
            o.order_id AS order_id,
            dc.customer_sk AS customer_sk,
            dp.product_sk AS product_sk,
            ds.store_sk AS store_sk,
            dd.date_key AS date_key,
            o.order_status AS order_status,
            ol.quantity AS quantity,
            ol.unit_price AS unit_price,
            ol.discount_pct AS discount_pct,
            ROUND(ol.quantity * ol.unit_price * (1 - COALESCE(ol.discount_pct, 0) / 100.0), 2)
                AS extended_amount
        FROM order_lines ol
        JOIN orders o ON o.order_id = ol.order_id
        LEFT JOIN dim_product dp ON dp.product_id = ol.product_id
        LEFT JOIN dim_store ds ON ds.store_id = o.store_id
        LEFT JOIN dim_date dd ON dd.full_date = CAST(o.order_ts AS DATE)
        LEFT JOIN dim_customer dc
            ON dc.customer_id = o.customer_id
           AND CAST(o.order_ts AS DATE) >= dc.effective_date
           AND (dc.end_date IS NULL OR CAST(o.order_ts AS DATE) <= dc.end_date)
        """,
    )
