# pipelines/

Core Python business logic for the platform, organized by medallion layer.
Airflow DAGs call into this package; this package has no Airflow dependency
itself, so it is independently unit-testable (see `tests/`).

- `ingestion/` — pulls data from source systems into the MinIO landing zone
  as NDJSON, regardless of source format. `base.py` holds the shared
  lifecycle (`BaseIngestion.run()`: extract -> land -> report); subclasses
  only implement `extract()`:
  - `api/customer_api.py` — paginated HTTP pull from the Customer API, retry
    (tenacity, exponential backoff) on connection errors/timeouts/5xx only —
    a 4xx fails fast, it's not transient.
  - `db/table_extract.py` — generic full-table extraction from the source
    Postgres, used for every DB-backed source (Sales orders/order_lines/
    products/stores, Inventory) — config-driven, not source-specific code;
    connection retry on `OperationalError`.
  - `files/supplier_files.py` — reads every file matching a glob in the
    configured drop directory (a real SFTP/file-share drop in production,
    `sample_data/suppliers/` locally).
  Each source is declared in `config/sources/*.yaml` and loaded via
  `config/sources.py`'s `SourceConfig` — adding a source means adding YAML,
  not new ingestion logic (beyond a new transport type, which is rare).
- `bronze/` — converts landing NDJSON into immutable, `batch_date`-partitioned
  Parquet. `writer.py` stores each raw record as a JSON string in a
  `payload` column (plus typed `_ingested_at`/`_source_file`/`_batch_date`
  technical columns) rather than flattening fields into typed Arrow columns
  — schema-on-read, so a field that's an int in one row and a string in
  another (real schema drift) never breaks the write. `reader.py` parses
  `payload` back into a flat dict for Silver/tooling to consume. Re-running
  for the same `(source, table, batch_date)` overwrites that partition
  (idempotent); no cleaning/casting/validation happens here — that's Silver.
- `silver/` — `runner.py`'s `run_silver_job(job_name, batch_date)` looks up
  and calls the right Spark job module — the cleaning/casting/dedup logic
  itself lives in `spark/jobs/` (see `spark/README.md`). `reader.py` reads a
  Silver Parquet partition back into memory (symmetric to
  `pipelines/bronze/reader.py`), used by the Gold builders below.
- `gold/` — builds the star schema: `dim_date` (generated calendar),
  `dim_product`/`dim_store` (Type 1, current-value), `dim_customer` (**Type
  2** — real insert-and-expire SCD logic: a changed tracked attribute
  expires the current row and inserts a new version with a fresh surrogate
  key, so `fact_sales` can join to whichever customer version was actually
  effective on the order date, not today's current one), `fact_sales`
  (grain: order line), and `daily_sales_summary` (KPI aggregate,
  cancelled/refunded orders excluded from revenue). `runner.py` builds every
  table in dependency order for a `batch_date`. `writer.py` reads/writes
  Gold tables — unlike Bronze/Silver, Gold tables are full current-state
  snapshots, not `batch_date`-partitioned. `schemas.py` holds explicit
  PyArrow schemas needed when a dimension is legitimately empty but still
  gets joined against (DuckDB won't register a zero-column relation).
  Built with DuckDB SQL directly rather than through dbt for now, since
  DuckDB requires no JVM/Docker to test — genuinely exercised locally with
  real SCD2 merge scenarios. Phase 13 formalizes this same logic as an
  actual dbt project (sources, snapshots for SCD2, tests, docs); this
  module may end up superseded by `dbt run` orchestration at that point, or
  kept as a non-dbt-dependent execution path — noted as an open design
  question for that phase.
- `storage.py` — the single MinIO/S3 client used by every layer (`LakeLayer`
  enum, `ObjectKey` path builder, `put_bytes`/`get_bytes`/`list_objects`).
  No other module constructs its own boto3 client or hardcodes a bucket name
  — this is what makes a future move from MinIO to real S3 a config change
  instead of a rewrite.

Every module here is config-driven (see `config/`) and takes no hardcoded
source paths, connection strings, or credentials.
