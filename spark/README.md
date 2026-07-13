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

## Performance tuning (Phase 12)

- **`spark.sql.shuffle.partitions` set to 4** in `get_spark_session` — the
  200 default is sized for cluster-scale shuffles; at this project's data
  volume it produces hundreds of near-empty shuffle tasks, all pure
  overhead. 4 is sized for local dev/this portfolio's volume specifically —
  a real deployment sizes this to executor core count and expected data
  volume, not a hardcoded constant.
- **Broadcast join** (`sales_order_lines_silver.py`) — its `clean()` now
  takes a `valid_orders_df` and does
  `order_lines.join(F.broadcast(orders.select("order_id")), on="order_id",
  how="left_semi")`, dropping order lines whose `order_id` doesn't exist in
  `orders` at all (a real data-quality gap the old NOT-NULL check didn't
  catch). `orders` (one row per order) is far smaller than `order_lines`
  (multiple rows per order) — broadcasting the small side to every executor
  avoids shuffling the larger table across the network, the standard
  fact/dimension-size-asymmetry case for a broadcast join. This makes
  `sales_order_lines_silver` depend on `sales_orders_silver` having already
  run for the same batch_date — reflected in
  `airflow/dags/silver_transform_dag.py`'s task graph.
- **`silver_to_spark_df`** (`jobs/common.py`) — loads an already-cleaned
  Silver table into Spark for that broadcast join, without
  `bronze_to_spark_df`'s stringify-everything step: Silver's whole point is
  that every column already has one consistent, cast type, so there's
  nothing to protect against there.
- **Explicit repartition before the window function** — `order_lines` is
  repartitioned by `order_line_id` immediately before `dedup_latest`
  (`Window.partitionBy` on the same key), so Spark plans one shuffle
  instead of two independent ones.
- **Caching — deliberately not used.** No job here reuses a DataFrame across
  more than one action (`clean()` runs once, `write_silver()` runs once);
  `.cache()` only pays off when a DataFrame feeds ≥2 actions, so adding it
  would just hold executor memory for zero benefit. Documenting *why not*
  matters more here than sprinkling `.cache()` around to look sophisticated.
- **Window functions** — already present since Phase 9 (`dedup_latest` is
  `row_number() OVER (PARTITION BY ... ORDER BY ... DESC)`), now paired with
  the repartitioning above for a complete before/after picture.

Built out in Phase 9 (core logic) and Phase 12 (the above).
