"""dim_customer — Type 2 (full history tracked on email/name/loyalty_tier
changes). This is the star-schema centerpiece: for each customer_id where a
tracked attribute changed since the last run, the current row is expired
(end_date set, is_current=false) and a new current row is inserted with a
fresh surrogate key — the standard insert-and-expire SCD2 pattern (the same
mechanism a dbt snapshot performs under the hood).
"""

from __future__ import annotations

from datetime import date, timedelta

import duckdb

from pipelines.gold.common import fetch_as_dicts, register_rows, surrogate_key_expr
from pipelines.gold.writer import read_gold_table
from pipelines.silver.reader import read_silver

TABLE_NAME = "dim_customer"


def _bootstrap(incoming: list[dict], batch_date: date) -> list[dict]:
    """First-ever run: every incoming customer becomes a brand-new current row."""
    con = duckdb.connect()
    register_rows(con, "incoming", incoming)
    return fetch_as_dicts(
        con,
        f"""
        SELECT
            {surrogate_key_expr(f"customer_id || '|' || '{batch_date.isoformat()}'")} AS customer_sk,
            customer_id, email, first_name, last_name, loyalty_tier,
            DATE '{batch_date.isoformat()}' AS effective_date,
            CAST(NULL AS DATE) AS end_date,
            true AS is_current
        FROM incoming
        """,  # noqa: S608 - batch_date is an internally-controlled date, not user input
    )


def build_dim_customer(batch_date: date) -> list[dict]:
    incoming = read_silver("customers", "customers", batch_date)
    if not incoming:
        return read_gold_table(TABLE_NAME)

    existing = read_gold_table(TABLE_NAME)
    if not existing:
        return _bootstrap(incoming, batch_date)

    expire_date = (batch_date - timedelta(days=1)).isoformat()
    con = duckdb.connect()
    register_rows(con, "incoming", incoming)
    register_rows(con, "existing", existing)

    return fetch_as_dicts(
        con,
        f"""
        WITH existing_current AS (SELECT * FROM existing WHERE is_current),
        changed_ids AS (
            SELECT i.customer_id
            FROM incoming i
            JOIN existing_current e USING (customer_id)
            WHERE i.email IS DISTINCT FROM e.email
               OR i.first_name IS DISTINCT FROM e.first_name
               OR i.last_name IS DISTINCT FROM e.last_name
               OR i.loyalty_tier IS DISTINCT FROM e.loyalty_tier
        ),
        new_ids AS (
            SELECT i.customer_id
            FROM incoming i
            LEFT JOIN existing_current e USING (customer_id)
            WHERE e.customer_id IS NULL
        ),
        expired AS (
            SELECT
                customer_sk, customer_id, email, first_name, last_name, loyalty_tier,
                effective_date,
                DATE '{expire_date}' AS end_date,
                false AS is_current
            FROM existing_current
            WHERE customer_id IN (SELECT customer_id FROM changed_ids)
        ),
        untouched_current AS (
            SELECT * FROM existing_current
            WHERE customer_id NOT IN (SELECT customer_id FROM changed_ids)
        ),
        historical AS (
            SELECT * FROM existing WHERE NOT is_current
        ),
        new_versions AS (
            SELECT
                {surrogate_key_expr(f"i.customer_id || '|' || '{batch_date.isoformat()}'")} AS customer_sk,
                i.customer_id, i.email, i.first_name, i.last_name, i.loyalty_tier,
                DATE '{batch_date.isoformat()}' AS effective_date,
                CAST(NULL AS DATE) AS end_date,
                true AS is_current
            FROM incoming i
            WHERE i.customer_id IN (SELECT customer_id FROM changed_ids)
               OR i.customer_id IN (SELECT customer_id FROM new_ids)
        )
        SELECT * FROM historical
        UNION ALL SELECT * FROM expired
        UNION ALL SELECT * FROM untouched_current
        UNION ALL SELECT * FROM new_versions
        """,  # noqa: S608 - batch_date/expire_date are internally-controlled dates, not user input
    )
