"""Shared DuckDB helpers for Gold builders.

DuckDB's `register()` only accepts pandas/Arrow/relation objects, not a raw
`list[dict]` — confirmed by hitting `Invalid Input Error: ... not suitable
for replacement scans` while building this. Every Gold builder goes through
`register_rows` so that conversion happens in exactly one place.
"""

from __future__ import annotations

import duckdb
import pyarrow as pa


def register_rows(
    con: duckdb.DuckDBPyConnection,
    name: str,
    rows: list[dict],
    empty_schema: pa.Schema | None = None,
) -> None:
    """Registers `rows` as a queryable DuckDB relation named `name`.

    When `rows` is empty, DuckDB refuses to register a zero-column table
    (`Invalid Input Error: ... must have at least one column` — hit this for
    real when a dimension legitimately has no rows yet). Callers that might
    pass an empty list for a table referenced in a JOIN must supply
    `empty_schema` so the relation still has the right column names/types.
    """
    if rows:
        arrow_table = pa.Table.from_pylist(rows)
    elif empty_schema is not None:
        arrow_table = pa.Table.from_pylist([], schema=empty_schema)
    else:
        raise ValueError(f"register_rows('{name}', []) needs an explicit empty_schema")
    con.register(name, arrow_table)


def fetch_as_dicts(con: duckdb.DuckDBPyConnection, query: str) -> list[dict]:
    result = con.sql(query)
    columns = [c[0] for c in result.description]
    return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


def surrogate_key_expr(expr: str) -> str:
    """DuckDB's `hash()` returns UBIGINT, which can exceed signed int64's
    range — PyArrow's schema inference then overflows converting it to a
    Python int (hit this for real generating dim_customer surrogate keys).
    Masking off the sign bit before casting to BIGINT keeps the value inside
    int64 while still being a well-distributed hash."""
    return f"CAST((hash({expr}) & 9223372036854775807) AS BIGINT)"
