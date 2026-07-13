select
    {{ dbt_utils.generate_surrogate_key(['s.store_id']) }} as store_sk,
    s.store_id,
    s.store_name,
    s.region,
    s.country,
    s.opened_date,
    r.timezone as region_timezone,
    r.regional_manager
from {{ ref('stg_stores') }} as s
left join {{ ref('region_metadata') }} as r on r.region = s.region
