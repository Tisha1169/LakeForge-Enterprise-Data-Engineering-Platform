"""dim_store — Type 1 (overwrite, no history tracked)."""

from __future__ import annotations

from datetime import date

import duckdb

from pipelines.gold.common import fetch_as_dicts, register_rows, surrogate_key_expr
from pipelines.silver.reader import read_silver


def build_dim_store(batch_date: date) -> list[dict]:
    rows = read_silver("sales_stores", "stores", batch_date)
    if not rows:
        return []

    con = duckdb.connect()
    register_rows(con, "stores", rows)
    return fetch_as_dicts(
        con,
        f"""
        SELECT
            {surrogate_key_expr("store_id")} AS store_sk,
            store_id,
            store_name,
            region,
            country,
            opened_date
        FROM stores
        """,
    )
