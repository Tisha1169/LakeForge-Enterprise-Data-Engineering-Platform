# spark/

PySpark jobs responsible for **Bronze -> Silver**: cleaning, normalizing,
deduplicating, and validating raw data at row level — work that benefits from
a general-purpose distributed engine rather than declarative SQL.

- `jobs/common.py` — shared `SparkSession` builder, the Bronze->Spark bridge
  (`bronze_to_spark_df`, which stringifies every field before building the
  DataFrame — the same schema-drift lesson from Bronze applies to Spark's
  own type inference), reusable cleaning transforms (`dedup_latest` via a
  window function, `standardize_date` via multi-format `coalesce`,
  `trim_and_lower`), and `write_silver`.
- `jobs/*_silver.py` — one job per Silver table (`customers_silver.py`,
  `sales_silver.py`, `sales_order_lines_silver.py`, `products_silver.py`,
  `stores_silver.py`, `inventory_silver.py`, `suppliers_silver.py`). Each has
  a `clean(df)` function (unit-testable) and a `run(batch_date)` entrypoint
  (`python -m spark.jobs.customers_silver 2024-01-01`), callable standalone
  or from `pipelines/silver/runner.py`.

I/O goes through `pipelines.storage`/`pipelines.bronze` (boto3) rather than
Spark's native S3A connector — avoids a version-matching dependency chain
(`hadoop-aws` + AWS SDK jars against the image's exact Hadoop build) that
can't be validated without a running cluster. Spark still does all the real
transformation work; only I/O is routed around Spark's own connector. This
is revisited in Phase 12 if native S3A reads are worth it at larger volumes.

`write_silver` builds its output Arrow table from `df.collect()` rather than
`DataFrame.toPandas()` — PySpark's `toPandas()` imports `distutils`, removed
in Python 3.12+, which breaks it outright under this project's Python 3.13
(caught via a real local test failure, not a hypothetical). Going through
`collect()` also sidesteps an unnecessary pandas round-trip.

`standardize_date` uses `try_to_date` rather than `to_date` — under Spark's
ANSI SQL mode (default since Spark 4.0; this project's Docker image pins
Spark 3.5, where ANSI defaults off, but a local `pip install pyspark` can
easily resolve to 4.x), `to_date` *raises* on a non-matching format instead
of returning `NULL`, which breaks the "try each format, first match wins"
`coalesce` pattern outright. `try_to_date` returns `NULL` on a mismatch
regardless of ANSI mode — caught by running the real Silver test suite
against a fresh, unpinned `pyspark` install.

**Local dev requires a JDK** (Spark needs a JVM) — e.g. `brew install
openjdk@17` on macOS, then `export JAVA_HOME=$(brew --prefix openjdk@17)`
before running `pytest tests/silver/` outside Docker. Inside
`docker compose`, the Spark containers already bundle their own JVM.

Built out in Phase 9 (core logic) and Phase 12 (performance-tuning patterns:
joins, window functions, partitioning, caching, broadcast joins).
