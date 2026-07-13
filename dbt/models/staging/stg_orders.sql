select
    order_id,
    customer_id,
    store_id,
    order_status,
    order_ts,
    updated_ts
from {{ source('silver', 'orders') }}
