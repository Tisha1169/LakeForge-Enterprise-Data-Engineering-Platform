# sample_data/

Small, synthetic seed datasets for local development and tests — never real
customer/business data.

- `customers/` — sample JSON payloads mimicking the Customer API response.
- `sales/` — sample rows for the Sales Postgres source (loaded via the
  Postgres init scripts in `docker/postgres/`).
- `inventory/` — sample rows for the Inventory Postgres source.
- `suppliers/` — sample CSV files mimicking the weekly supplier file drop.

These are what `docker compose up` seeds the local Postgres sources with, so
the entire pipeline is runnable end-to-end with zero external dependencies.
