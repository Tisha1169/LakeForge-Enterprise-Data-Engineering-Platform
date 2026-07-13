select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk,
    product_id,
    sku,
    product_name,
    category,
    unit_price
from {{ ref('stg_products') }}
