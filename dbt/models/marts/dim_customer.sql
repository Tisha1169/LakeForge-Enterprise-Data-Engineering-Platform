-- Wraps the dim_customer_snapshot (the real SCD2 mechanism) into
-- business-friendly column names, plus a stable surrogate key.
select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }} as customer_sk,
    customer_id,
    email,
    first_name,
    last_name,
    loyalty_tier,
    dbt_valid_from as effective_date,
    dbt_valid_to as end_date,
    (dbt_valid_to is null) as is_current
from {{ ref('dim_customer_snapshot') }}
