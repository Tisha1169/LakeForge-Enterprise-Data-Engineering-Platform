"""dim_product — Type 1 (overwrite, no history tracked). Price history could
be a future SCD Type 2 extension; out of scope at this stage."""

from __future__ import annotations

from datetime import date

import duckdb

from pipelines.gold.common import fetch_as_dicts, register_rows, surrogate_key_expr
from pipelines.silver.reader import read_silver


def build_dim_product(batch_date: date) -> list[dict]:
    rows = read_silver("sales_products", "products", batch_date)
    if not rows:
        return []

    con = duckdb.connect()
    register_rows(con, "products", rows)
    return fetch_as_dicts(
        con,
        f"""
        SELECT
            {surrogate_key_expr("product_id")} AS product_sk,
            product_id,
            sku,
            product_name,
            category,
            unit_price
        FROM products
        """,
    )
