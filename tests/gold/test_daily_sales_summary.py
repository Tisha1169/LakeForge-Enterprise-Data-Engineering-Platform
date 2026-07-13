from pipelines.gold.daily_sales_summary import build_daily_sales_summary


def test_daily_sales_summary_excludes_cancelled_and_refunded_orders():
    fact_rows = [
        {
            "order_line_id": 1,
            "order_id": 1,
            "store_sk": 1,
            "date_key": 20240101,
            "order_status": "completed",
            "quantity": 2,
            "extended_amount": 20.0,
        },
        {
            "order_line_id": 2,
            "order_id": 2,
            "store_sk": 1,
            "date_key": 20240101,
            "order_status": "cancelled",
            "quantity": 5,
            "extended_amount": 100.0,
        },
        {
            "order_line_id": 3,
            "order_id": 3,
            "store_sk": 1,
            "date_key": 20240101,
            "order_status": "completed",
            "quantity": 1,
            "extended_amount": 10.0,
        },
    ]
    dim_store_rows = [
        {
            "store_sk": 1,
            "store_id": 1,
            "store_name": "Downtown",
            "region": "NE",
            "country": "USA",
            "opened_date": None,
        }
    ]

    rows = build_daily_sales_summary(fact_rows, dim_store_rows)

    assert len(rows) == 1
    row = rows[0]
    assert row["total_orders"] == 2  # order 2 (cancelled) excluded
    assert row["total_quantity"] == 3
    assert row["total_revenue"] == 30.0
    assert row["avg_order_value"] == 15.0


def test_daily_sales_summary_returns_empty_for_no_fact_rows():
    assert build_daily_sales_summary([], []) == []
