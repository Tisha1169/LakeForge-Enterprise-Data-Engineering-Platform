# pipelines/gold/

The star schema build (dimensions, fact, aggregate, SCD Type 2) — see
[pipelines/README.md](../README.md) for the full breakdown of `dim_date.py`,
`dim_customer.py`, `dim_product.py`, `dim_store.py`, `fact_sales.py`,
`daily_sales_summary.py`, `common.py`, `schemas.py`, `writer.py`, and
`runner.py`. A parallel dbt implementation of the same schema lives in
[`dbt/`](../../dbt/README.md).
