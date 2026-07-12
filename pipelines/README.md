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
    Postgres, used for both Sales and Inventory (config-driven, not
    source-specific code); connection retry on `OperationalError`.
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
- `silver/` — thin orchestration around the PySpark jobs in `spark/jobs/`
  (job submission, parameter resolution) — the transformation logic itself
  lives in `spark/`.
- `gold/` — thin orchestration around dbt runs (invoking `dbt run`/`dbt
  test` for the relevant model selection) — the transformation logic itself
  lives in `dbt/`.
- `storage.py` — the single MinIO/S3 client used by every layer (`LakeLayer`
  enum, `ObjectKey` path builder, `put_bytes`/`get_bytes`/`list_objects`).
  No other module constructs its own boto3 client or hardcodes a bucket name
  — this is what makes a future move from MinIO to real S3 a config change
  instead of a rewrite.

Every module here is config-driven (see `config/`) and takes no hardcoded
source paths, connection strings, or credentials.
