select
    customer_id,
    email,
    first_name,
    last_name,
    loyalty_tier,
    signup_date,
    _ingested_at
from {{ source('silver', 'customers') }}
