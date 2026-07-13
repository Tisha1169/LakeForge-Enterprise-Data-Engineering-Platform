# dbt/

dbt Core project responsible for **Silver -> Gold**: turning cleaned Silver
tables into business-ready star-schema Gold tables using testable, documented
SQL. Formalizes the same star schema `pipelines/gold/` builds by hand in
Python/DuckDB (Phase 10) — see docs/architecture.md for why both exist.

- `models/staging/` — `stg_*.sql` (thin, one per Silver table: rename/select
  only, no logic) and `_sources.yml` (declares the five Silver tables via
  DuckDB's `external_location`, pointed at Parquet through
  `OPENLAKE_SILVER_BASE` — defaults to `s3://openlake-silver` in production,
  overridable to a local fixture directory for dev/testing).
- `models/marts/` — the star schema: `dim_date` (via `dbt_utils.date_spine`),
  `dim_product`/`dim_store` (Type 1, `dim_store` also joins the
  `region_metadata` seed), `dim_customer` (wraps `dim_customer_snapshot`
  into business-friendly columns + a `dbt_utils.generate_surrogate_key`),
  `fact_sales` (grain: order line, point-in-time join to `dim_customer` —
  see the file's header comment for a real granularity bug this caught),
  `daily_sales_summary` (KPI aggregate). `_marts.yml` / `_staging.yml` hold
  schema tests (`unique`, `not_null`, `relationships`).
- `snapshots/dim_customer_snapshot.sql` — the actual SCD Type 2 mechanism
  (dbt's `check` strategy, diffing `email`/`first_name`/`last_name`/
  `loyalty_tier`), superseding the hand-rolled insert-and-expire SQL Phase 10
  wrote in `pipelines/gold/dim_customer.py` before this project had a real
  dbt setup to put it in.
- `seeds/region_metadata.csv` — small static reference data (timezone,
  regional manager) with no operational source of its own — joined into
  `dim_store`.
- `tests/` — one custom singular test
  (`assert_fact_sales_extended_amount_non_negative.sql`); schema tests live
  inline in `_staging.yml`/`_marts.yml`.
- `packages.yml` / `package-lock.yml` — `dbt_utils` (surrogate keys,
  `date_spine`) — a real, standard dbt package, not a hand-rolled macro.
- `profiles.yml` — `dbt-duckdb`, two targets (`dev`/`prod`), all connection
  details templated through `env_var()`. DuckDB was chosen (over dbt-postgres,
  also installed) because it can query Parquet directly via `read_parquet()`
  — no separate load step is needed to get Silver's data queryable.

## A schema-drift-shaped gotcha this design already accounts for

Every Silver write is a **full snapshot for that batch_date**, not an
incremental delta (see `pipelines/silver/reader.py`). A naive recursive glob
over every day's Parquet file in `external_location` would make a
customer/order/etc. that appears in ten daily batches show up ten times.
Each source's `external_location` uses DuckDB's Hive-partitioning support
(paths are `batch_date=<date>/part-0.parquet`) plus `qualify dense_rank()
over (order by batch_date desc) = 1` to filter to only the latest batch.

The first version of this used `where batch_date = (select max(batch_date)
from read_parquet(...))` instead — reading the same Parquet glob twice (once
for the outer query, once for the correlated subquery) reliably **crashes**
DuckDB 1.10.1's statistics propagator (`INTERNAL Error: Attempted to access
index 7 within vector of size 7`), reproduced directly against `duckdb-python`
independent of dbt entirely — a genuine engine bug, not a dbt or project-code
issue. The `qualify`/window-function form reads the glob once and produces
identical results without it.

## Verified locally

Installed `dbt-core`+`dbt-duckdb` and generated small local Parquet fixtures
(`scripts/generate_dbt_local_fixtures.py`) matching Silver's exact on-disk
layout, then ran the real sequence: `dbt deps && dbt seed && dbt snapshot &&
dbt run && dbt test && dbt docs generate`. All 11 models built, all 28 tests
passed, docs generated. Ran `dbt snapshot` **twice** (a bootstrap batch, then
a batch with a genuine loyalty-tier change) and inspected
`snapshots.dim_customer_snapshot` directly — confirmed a changed customer
got a real expire-and-insert (two rows, correct `dbt_valid_from`/
`dbt_valid_to`), an unchanged customer stayed as one row (no spurious
version), and a brand-new customer only appeared from the second run.

This also caught a real bug in `fact_sales.sql`: dbt snapshots stamp
`dbt_valid_from`/`dbt_valid_to` with actual wall-clock execution time (full
timestamp precision), not a business/batch date — unlike
`pipelines/gold/dim_customer.py`'s Python implementation, which explicitly
sets `effective_date = batch_date` (day granularity). The original join
truncated `order_ts` to a date before comparing against `dim_customer`'s
timestamp-precision `effective_date`/`end_date`, so an order placed a few
minutes after a real snapshot run compared against "midnight" and silently
lost the match (`customer_sk` came back `NULL`). Fixed by comparing the full
timestamp for the customer join (keeping date-only truncation only for the
`dim_date` lookup, which is genuinely daily-grain) — verified by placing one
order between two real snapshot runs and one after, confirming they resolved
to the bronze-tier and gold-tier customer versions respectively.

## Local dev

```bash
export DBT_PROFILES_DIR=dbt
export DBT_DUCKDB_PATH=/tmp/openlake_dev.duckdb
export OPENLAKE_SILVER_BASE=/tmp/dbt_fixtures   # or a real s3://openlake-silver path

python scripts/generate_dbt_local_fixtures.py /tmp/dbt_fixtures 2024-01-01
cd dbt && dbt deps && dbt seed && dbt snapshot && dbt run && dbt test
```

Why dbt for this layer and not PySpark: business logic here is best expressed
as declarative, testable SQL that a data analyst can also read and extend —
not general-purpose distributed compute.
