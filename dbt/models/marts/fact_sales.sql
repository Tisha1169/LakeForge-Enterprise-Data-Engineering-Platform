-- Grain: one row per order line. Point-in-time join to dim_customer — same
-- reasoning as pipelines/gold/fact_sales.py: an order's customer_sk is
-- whichever dim_customer version was effective on the order date, not
-- necessarily today's current version.
--
-- The customer join compares against the FULL order_ts timestamp, not a
-- date-truncated one: dim_customer.effective_date/end_date come from dbt
-- snapshots' dbt_valid_from/dbt_valid_to, which carry real wall-clock
-- precision (unlike pipelines/gold's Python implementation, where
-- effective_date is a plain DATE derived from batch_date). Truncating
-- order_ts to a date here compares "midnight" against a same-day
-- timestamped tier change and silently loses same-day transitions — caught
-- by testing an order placed a few minutes after a real snapshot run, which
-- returned NULL customer_sk until this was fixed. dim_date's join still
-- truncates to a date, since its grain is genuinely daily.
select
    ol.order_line_id,
    o.order_id,
    dc.customer_sk,
    dp.product_sk,
    ds.store_sk,
    dd.date_key,
    o.order_status,
    ol.quantity,
    ol.unit_price,
    ol.discount_pct,
    round(ol.quantity * ol.unit_price * (1 - coalesce(ol.discount_pct, 0) / 100.0), 2) as extended_amount
from {{ ref('stg_order_lines') }} as ol
inner join {{ ref('stg_orders') }} as o on o.order_id = ol.order_id
left join {{ ref('dim_product') }} as dp on dp.product_id = ol.product_id
left join {{ ref('dim_store') }} as ds on ds.store_id = o.store_id
left join {{ ref('dim_date') }} as dd on dd.full_date = cast(o.order_ts as date)
left join {{ ref('dim_customer') }} as dc
    on dc.customer_id = o.customer_id
    and cast(o.order_ts as timestamp) >= dc.effective_date
    and (dc.end_date is null or cast(o.order_ts as timestamp) < dc.end_date)
