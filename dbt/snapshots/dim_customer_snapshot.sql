{% snapshot dim_customer_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['email', 'first_name', 'last_name', 'loyalty_tier'],
    )
}}

-- This IS the SCD Type 2 mechanism (Phase 10 hand-rolled the same
-- insert-and-expire pattern in Python/DuckDB before this project had a real
-- dbt project to put it in — see docs/architecture.md). dbt's snapshot
-- diffs check_cols against the current row on every run and automatically
-- writes dbt_valid_from/dbt_valid_to; dim_customer.sql below renames those
-- into the business-friendly columns fact_sales actually joins against.
select * from {{ source('silver', 'customers') }}

{% endsnapshot %}
