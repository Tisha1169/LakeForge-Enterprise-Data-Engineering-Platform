-- KPI aggregate, grain: (date_key, store_id). Cancelled/refunded orders are
-- excluded from revenue — counting them would overstate actual performance.
select
    f.date_key,
    ds.store_id,
    ds.store_name,
    count(distinct f.order_id) as total_orders,
    sum(f.quantity) as total_quantity,
    round(sum(f.extended_amount), 2) as total_revenue,
    round(sum(f.extended_amount) / nullif(count(distinct f.order_id), 0), 2) as avg_order_value
from {{ ref('fact_sales') }} as f
left join {{ ref('dim_store') }} as ds on ds.store_sk = f.store_sk
where f.order_status not in ('cancelled', 'refunded')
group by f.date_key, ds.store_id, ds.store_name
