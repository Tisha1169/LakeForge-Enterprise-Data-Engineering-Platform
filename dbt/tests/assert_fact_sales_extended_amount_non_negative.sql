-- Custom singular test: fails (returns rows) if any fact_sales row has a
-- negative extended_amount -- would indicate a discount_pct > 100 or a
-- negative quantity/unit_price slipping through Silver's casting.
select *
from {{ ref('fact_sales') }}
where extended_amount < 0
