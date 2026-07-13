select
    order_line_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_pct
from {{ source('silver', 'order_lines') }}
