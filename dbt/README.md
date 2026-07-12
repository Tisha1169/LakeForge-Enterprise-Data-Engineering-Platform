# dbt/

dbt Core project responsible for **Silver -> Gold**: turning cleaned Silver
tables into business-ready star-schema Gold tables using testable, documented
SQL.

Will contain (Phase 13):
- `models/silver/` — dbt *sources* pointing at Silver tables (no
  transformation, just declaring what exists and its freshness expectations).
- `models/gold/` — fact and dimension models, incremental where appropriate.
- `seeds/` — small static reference data (e.g. currency codes, store region
  lookup) checked into the repo as CSV.
- `snapshots/` — SCD Type 2 history for dimensions that change over time
  (e.g. customer address changes).
- `tests/` — schema tests (uniqueness, not-null, relationships) and custom
  SQL tests.
- `macros/` — reusable SQL/Jinja (e.g. surrogate key generation).

Why dbt for this layer and not PySpark: business logic here is best expressed
as declarative, testable SQL that a data analyst can also read and extend —
not general-purpose distributed compute.
