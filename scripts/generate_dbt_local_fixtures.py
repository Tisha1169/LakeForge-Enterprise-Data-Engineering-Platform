"""Generates small local Parquet fixtures matching Silver's exact on-disk
layout (source/table/batch_date=<date>/part-0.parquet), so the dbt project
can be run and tested locally (`dbt seed && dbt snapshot && dbt run && dbt
test`) against OPENLAKE_SILVER_BASE pointed at a local directory instead of
real MinIO/S3.

Writes two customer batches (2024-01-01, 2024-01-08) with a genuine tier
change on customer_id=1, so running `dbt snapshot` twice — once per batch —
exercises the SCD Type 2 change-tracking path for real, not just the
bootstrap case. Usage: see dbt/README.md.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def _write(base: Path, source: str, table: str, batch_date: str, rows: list[dict]) -> None:
    path = base / source / table / f"batch_date={batch_date}" / "part-0.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)
    print(f"wrote {len(rows)} rows -> {path}")


def generate(base: Path, customers_batch_date: str) -> None:
    if customers_batch_date == "2024-01-01":
        customers = [
            {
                "customer_id": 1,
                "email": "alex@example.com",
                "first_name": "Alex",
                "last_name": "Smith",
                "loyalty_tier": "bronze",
                "signup_date": "2023-02-02",
                "_ingested_at": "2024-01-01T00:00:00",
            },
            {
                "customer_id": 2,
                "email": "dana@example.com",
                "first_name": "Dana",
                "last_name": "Kim",
                "loyalty_tier": "silver",
                "signup_date": "2023-05-11",
                "_ingested_at": "2024-01-01T00:00:00",
            },
        ]
    else:
        customers = [
            {
                "customer_id": 1,
                "email": "alex@example.com",
                "first_name": "Alex",
                "last_name": "Smith",
                "loyalty_tier": "gold",  # changed -> dbt snapshot should version this
                "signup_date": "2023-02-02",
                "_ingested_at": customers_batch_date + "T00:00:00",
            },
            {
                "customer_id": 2,
                "email": "dana@example.com",
                "first_name": "Dana",
                "last_name": "Kim",
                "loyalty_tier": "silver",  # unchanged
                "signup_date": "2023-05-11",
                "_ingested_at": customers_batch_date + "T00:00:00",
            },
            {
                "customer_id": 3,  # brand new customer
                "email": "priya@example.com",
                "first_name": "Priya",
                "last_name": "Patel",
                "loyalty_tier": "bronze",
                "signup_date": "2024-01-05",
                "_ingested_at": customers_batch_date + "T00:00:00",
            },
        ]
    _write(base, "customers", "customers", customers_batch_date, customers)

    orders = [
        {
            "order_id": 100,
            "customer_id": 1,
            "store_id": 1,
            "order_status": "completed",
            "order_ts": "2024-01-03 09:00:00",
            "updated_ts": "2024-01-03 09:10:00",
        },
        {
            "order_id": 101,
            "customer_id": 2,
            "store_id": 1,
            "order_status": "completed",
            "order_ts": "2024-01-10 14:00:00",
            "updated_ts": "2024-01-10 14:05:00",
        },
        {
            "order_id": 102,
            "customer_id": 1,
            "store_id": 2,
            "order_status": "cancelled",
            "order_ts": "2024-01-11 10:00:00",
            "updated_ts": "2024-01-11 10:20:00",
        },
    ]
    _write(base, "sales", "orders", "2024-01-11", orders)

    order_lines = [
        {"order_line_id": 1, "order_id": 100, "product_id": 101, "quantity": 2, "unit_price": 24.99, "discount_pct": 0},
        {"order_line_id": 2, "order_id": 100, "product_id": 102, "quantity": 1, "unit_price": 79.99, "discount_pct": 10},
        {"order_line_id": 3, "order_id": 101, "product_id": 101, "quantity": 1, "unit_price": 24.99, "discount_pct": 0},
        {"order_line_id": 4, "order_id": 102, "product_id": 103, "quantity": 3, "unit_price": 34.50, "discount_pct": 0},
    ]
    _write(base, "sales_order_lines", "order_lines", "2024-01-11", order_lines)

    products = [
        {"product_id": 101, "sku": "SKU-101", "product_name": "Wireless Mouse", "category": "Electronics", "unit_price": 24.99},
        {"product_id": 102, "sku": "SKU-102", "product_name": "Mechanical Keyboard", "category": "Electronics", "unit_price": 79.99},
        {"product_id": 103, "sku": "SKU-103", "product_name": "USB-C Hub", "category": "Electronics", "unit_price": 34.50},
    ]
    _write(base, "sales_products", "products", "2024-01-11", products)

    stores = [
        {"store_id": 1, "store_name": "Downtown Flagship", "region": "Northeast", "country": "USA", "opened_date": "2015-03-01"},
        {"store_id": 2, "store_name": "Westside Mall", "region": "West", "country": "USA", "opened_date": "2017-06-15"},
    ]
    _write(base, "sales_stores", "stores", "2024-01-11", stores)


if __name__ == "__main__":
    base_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("dbt_fixtures")
    batch = sys.argv[2] if len(sys.argv) > 2 else "2024-01-01"
    generate(base_dir, batch)
