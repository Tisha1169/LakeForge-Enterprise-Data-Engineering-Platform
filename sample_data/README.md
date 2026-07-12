# sample_data/

Small, synthetic seed datasets for local development and tests — never real
customer/business data.

- `customers/customers.json` — 52 synthetic customer records served by the
  `customer-api` mock service (`docker/customer-api/`) — the actual payload
  the Customer API ingestion pulls from, paginated. Deliberately includes a
  null email and inconsistent date formats.
- `sales/`, `inventory/` — reserved for future file-based fixtures; the
  actual Sales/Inventory seed data currently lives directly in the Postgres
  init scripts (`docker/postgres/init-source/03_seed_sales.sql`,
  `04_seed_inventory.sql`) since it needs to respect foreign keys.
- `suppliers/supplier_catalog_2024W01.csv` — sample weekly supplier catalog
  drop, read directly by the Supplier File ingestion. Deliberately includes
  a missing SKU, a missing `updated_at`, and an inconsistent date format
  (`01/03/2024` vs `2024-01-03`) — real problems for Silver-layer cleaning
  and Great Expectations to catch, not already-clean fixtures.

Postgres seed data loads automatically on first `docker compose up` via the
init scripts; the Customer API and Supplier File sources are read directly
from this directory by their respective ingestion modules.
