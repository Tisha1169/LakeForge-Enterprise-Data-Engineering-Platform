"""Explicit PyArrow schemas for Gold dimension tables.

Needed anywhere a dimension might legitimately be empty but still gets
registered as a DuckDB relation for a JOIN — see
`common.register_rows` for why a schema must be supplied explicitly in
that case.
"""

from __future__ import annotations

import pyarrow as pa

DIM_CUSTOMER_SCHEMA = pa.schema(
    [
        ("customer_sk", pa.int64()),
        ("customer_id", pa.int64()),
        ("email", pa.string()),
        ("first_name", pa.string()),
        ("last_name", pa.string()),
        ("loyalty_tier", pa.string()),
        ("effective_date", pa.date32()),
        ("end_date", pa.date32()),
        ("is_current", pa.bool_()),
    ]
)

DIM_PRODUCT_SCHEMA = pa.schema(
    [
        ("product_sk", pa.int64()),
        ("product_id", pa.int64()),
        ("sku", pa.string()),
        ("product_name", pa.string()),
        ("category", pa.string()),
        ("unit_price", pa.float64()),
    ]
)

DIM_STORE_SCHEMA = pa.schema(
    [
        ("store_sk", pa.int64()),
        ("store_id", pa.int64()),
        ("store_name", pa.string()),
        ("region", pa.string()),
        ("country", pa.string()),
        ("opened_date", pa.date32()),
    ]
)

DIM_DATE_SCHEMA = pa.schema(
    [
        ("date_key", pa.int64()),
        ("full_date", pa.date32()),
        ("year", pa.int64()),
        ("quarter", pa.int64()),
        ("month", pa.int64()),
        ("month_name", pa.string()),
        ("day", pa.int64()),
        ("day_of_week", pa.int64()),
        ("day_name", pa.string()),
        ("is_weekend", pa.bool_()),
    ]
)
