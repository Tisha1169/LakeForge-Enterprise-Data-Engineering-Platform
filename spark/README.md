# spark/

PySpark jobs responsible for **Bronze -> Silver**: cleaning, normalizing,
deduplicating, and validating raw data at row level — work that benefits from
a general-purpose distributed engine rather than declarative SQL.

- `jobs/` — one Spark job per Silver table (e.g. `sales_silver.py`,
  `customers_silver.py`). Each job is a parameterized script (source bronze
  path, target silver path, batch date) runnable standalone or from an
  Airflow task.

Built out in Phase 9 (core logic) and Phase 12 (performance-tuning patterns:
joins, window functions, partitioning, caching, broadcast joins).
