select
    product_id,
    sku,
    product_name,
    category,
    unit_price
from {{ source('silver', 'products') }}
