select
    store_id,
    store_name,
    region,
    country,
    opened_date
from {{ source('silver', 'stores') }}
