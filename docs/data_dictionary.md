# Data Dictionary

Covers Silver and Gold — Bronze is deliberately schema-on-read (raw JSON
payload, no fixed column contract; see `pipelines/bronze/writer.py`) and
isn't a stable enough contract to document as a dictionary.

## Silver layer

### `customers` (source: `customers`)

| Column | Type | Notes |
|---|---|---|
| `customer_id` | int | Natural key. Rows with a null value are dropped in Silver. |
| `email` | string | Lowercased, trimmed; empty string nulled out. |
| `first_name` | string | Trimmed. |
| `last_name` | string | Trimmed. |
| `loyalty_tier` | string | Not constrained at Silver (constrained by `data_quality/suites.py`'s `silver_customers_suite` instead — `bronze/silver/gold/platinum`). |
| `signup_date` | date | Standardized from 3 known raw formats via `try_to_date` + `coalesce`. |
| `_ingested_at`, `_source_file`, `_batch_date` | string | Bronze audit columns, passed through. |

Dedup: one row per `customer_id`, most recently *ingested* wins (Silver is
not itself SCD2 — see Gold `dim_customer` for history).

### `orders` (source: `sales`, table `orders`)

| Column | Type | Notes |
|---|---|---|
| `order_id` | int | Natural key. Null rows dropped. |
| `customer_id` | int | FK to `customers.customer_id`. |
| `store_id` | int | FK to `stores.store_id`. |
| `order_status` | string | `pending`/`completed`/`cancelled`/`refunded`; anything else becomes `unknown` rather than passing through unvalidated. |
| `order_ts` | timestamp | When the order was placed. |
| `updated_ts` | timestamp | Dedup key — orders are mutable in the source, so "latest `updated_ts` wins," not "latest ingested." |

### `order_lines` (source: `sales_order_lines`, table `order_lines`)

| Column | Type | Notes |
|---|---|---|
| `order_line_id` | int | Natural key, grain of this table. |
| `order_id` | int | FK to `orders.order_id` — enforced via a broadcast semi-join at Silver (Phase 12), not just a null check; a line referencing a nonexistent order is dropped. |
| `product_id` | int | FK to `products.product_id`. |
| `quantity` | int | |
| `unit_price` | double | |
| `discount_pct` | double | 0–100. |

### `products` (source: `sales_products`, table `products`)

| Column | Type | Notes |
|---|---|---|
| `product_id` | int | Natural key. |
| `sku` | string | Dropped if empty (an incomplete catalog line is unusable). |
| `product_name` | string | |
| `category` | string | |
| `unit_price` | double | |

### `stores` (source: `sales_stores`, table `stores`)

| Column | Type | Notes |
|---|---|---|
| `store_id` | int | Natural key. |
| `store_name` | string | |
| `region` | string | |
| `country` | string | |
| `opened_date` | date | |

### `stock_snapshots` (source: `inventory`, table `stock_snapshots`)

| Column | Type | Notes |
|---|---|---|
| `snapshot_id` | long | |
| `store_id`, `product_id`, `snapshot_date` | int, int, date | Composite grain — one row per store/product/day. |
| `quantity_on_hand` | int | |
| `reorder_point` | int | |
| `supplier_id` | int | |

### `suppliers` (source: `suppliers`, table `suppliers`)

| Column | Type | Notes |
|---|---|---|
| `supplier_id` | int | |
| `product_sku` | string | Dropped if empty. |
| `unit_cost` | double | |
| `lead_time_days` | int | |
| `updated_at` | date | Standardized from 2 known raw formats; left `NULL` if genuinely missing rather than guessed. |

## Gold layer (star schema)

Built two ways — hand-rolled Python/DuckDB (`pipelines/gold/`, Phase 10) and
a real dbt project (`dbt/models/marts/`, Phase 13); same shape, same column
names, two implementations (see `docs/architecture.md` for why).

### `dim_date` — generated calendar, no source table

| Column | Type |
|---|---|
| `date_key` | int (`YYYYMMDD`) — the join key fact tables use |
| `full_date` | date |
| `year`, `quarter`, `month`, `day` | int |
| `month_name`, `day_name` | string |
| `day_of_week` | int (1=Monday..7=Sunday, ISO) |
| `is_weekend` | boolean |

### `dim_customer` — **Type 2** (SCD)

| Column | Type | Notes |
|---|---|---|
| `customer_sk` | bigint | Surrogate key — a **new** value per version of a customer, not stable across tier changes. |
| `customer_id` | int | Natural key — stable across versions. |
| `email`, `first_name`, `last_name`, `loyalty_tier` | string | Tracked columns — a change to any of these creates a new version. |
| `effective_date` | date (Python impl) / timestamp (dbt) | When this version became current. |
| `end_date` | date / timestamp, nullable | `NULL` = this is the current version. |
| `is_current` | boolean | Convenience flag, `end_date IS NULL`. |

### `dim_product` / `dim_store` — Type 1 (no history)

| Column | Type |
|---|---|
| `product_sk` / `store_sk` | bigint, surrogate key (stable hash of the natural key) |
| `product_id` / `store_id` | int, natural key |
| *(remaining columns pass through from Silver, plus `dim_store` joins `region_metadata` for `region_timezone`/`regional_manager` in the dbt version — see `dbt/seeds/region_metadata.csv`)* | |

### `fact_sales` — grain: one row per order line

| Column | Type | Notes |
|---|---|---|
| `order_line_id` | int | Grain. |
| `order_id` | int | |
| `customer_sk` | bigint, nullable | **Point-in-time** join to `dim_customer` — whichever version was effective on `order_ts`, not necessarily today's current one. |
| `product_sk`, `store_sk` | bigint, nullable | Type 1 current-value joins. |
| `date_key` | int, nullable | Joins `dim_date` on the order's calendar date. |
| `order_status` | string | Passed through from Silver `orders`. |
| `quantity`, `unit_price`, `discount_pct` | int/double/double | Passed through from Silver `order_lines`. |
| `extended_amount` | double | `quantity * unit_price * (1 - discount_pct / 100)`, rounded to 2dp. |

### `daily_sales_summary` — grain: `(date_key, store_id)`

| Column | Type | Notes |
|---|---|---|
| `date_key`, `store_id`, `store_name` | int, int, string | Grain + display. |
| `total_orders` | int | `COUNT(DISTINCT order_id)`. |
| `total_quantity` | int | |
| `total_revenue` | double | Sum of `extended_amount`. **`cancelled`/`refunded` orders excluded** — counting them would overstate actual sales performance. |
| `avg_order_value` | double, nullable | `total_revenue / total_orders`; `NULL` when there are no orders for the grain (not a divide-by-zero error). |
