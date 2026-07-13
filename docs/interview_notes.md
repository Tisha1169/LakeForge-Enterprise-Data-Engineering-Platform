# Interview Notes

Talking points, likely questions, resume bullets, and — the most genuinely
useful section — real mistakes made and fixed while building this project,
not generic advice. Organized by phase; skip to what's relevant.

## Recruiter-facing summary

OpenLake is a medallion-architecture (Bronze/Silver/Gold) data lakehouse
processing heterogeneous retail data — a REST API, two operational Postgres
databases, and CSV file drops — into a tested, orchestrated, documented
analytics platform. It's built to be run, not just read: every layer is
verified against real execution (real Spark, real dbt runs, real SQL
against SQLite/DuckDB, a real Great Expectations checkpoint), and several
real bugs were found and fixed by actually running things rather than
reviewing code by eye. The full build log — including bugs found, why they
happened, and how they were fixed — is in the commit history and this doc.

## Architecture & design (Phases 1, 10, 13)

**Q: Why medallion architecture instead of straight-to-warehouse?**
Bronze is immutable and append-only, so any bug in Silver/Gold logic can be
fixed and replayed from Bronze without re-hitting source systems (some of
which, like a REST API, may not even support historical replay). A broken
transformation corrupts Silver/Gold, never the raw historical record.

**Q: Why do both a hand-rolled Python/DuckDB Gold layer AND a real dbt
project exist?**
Sequential, not redundant. Phase 10 needed something testable with zero
external services before a dbt setup existed — DuckDB is embedded, no
JVM/warehouse connection required. Phase 13 formalizes the same star schema
as an actual dbt project (sources, a real `dbt snapshot` for SCD2, schema
tests, generated docs) — what an analytics team would actually maintain
long-term. Keeping both demonstrates understanding of what a snapshot
mechanism does *underneath*, not just how to invoke one.

**Q: Why MinIO instead of a cloud object store from day one?**
Same S3 API — `pipelines/storage.py` never changes when pointed at real S3
later (see `docs/deployment_guide.md`). Zero cloud cost during development,
fully offline-capable.

## Real bugs found and fixed (the "common mistakes" section)

These are first-hand, not textbook — every one of these broke a real run
during this build, was diagnosed, and fixed. Worth knowing if you're
building something similar.

1. **Flattening raw JSON into typed Parquet columns eagerly breaks on
   schema drift.** PyArrow's `Table.from_pylist` infers one type per column
   across *all* rows; a field that's an `int` in one row and a `str` in
   another (real raw-data messiness) throws `ArrowInvalid`. Fix: Bronze
   stores each record as a JSON string in a `payload` column (schema-on-
   read) plus typed technical columns — never flatten raw, unvalidated data
   into typed columns before you've explicitly cast it. (Phase 8)

2. **The same mistake resurfaces in Spark's own schema inference** — a
   Bronze field with mixed types across rows breaks `spark.createDataFrame`
   the same way. Fix: stringify every field before building the DataFrame,
   cast explicitly and safely afterward. (Phase 9)

3. **`DataFrame.toPandas()` silently depends on `distutils`**, removed in
   Python 3.12+. If your project targets a newer Python than your Spark
   image assumes, this breaks the moment you touch it. Fix: build the
   output Arrow table directly from `df.collect()` — also removes an
   unnecessary pandas round-trip. (Phase 9)

4. **`to_date()` behaves differently across Spark versions depending on
   ANSI SQL mode** — raises on a non-matching format under ANSI (default
   since Spark 4.0) instead of returning `NULL`, which silently breaks a
   "try each format, first match wins" `coalesce` pattern. Fix:
   `try_to_date()` returns `NULL` on a mismatch regardless of ANSI mode —
   more robust across versions, not just a workaround for one. Caught by
   running the test suite against a fresh, unpinned `pyspark` install
   alongside the Docker image's pinned 3.5. (Phase 12, surfaced testing
   Phase 9 code)

5. **A project directory with the same name as a pip package silently
   shadows submodule imports.** `great_expectations/context.py`'s own
   `from great_expectations.context import get_context` tried to find
   `context` *inside the real pip library* once it was installed, since a
   regular package wins over a same-named namespace-package directory for
   the top-level name. `import great_expectations` alone looked fine — the
   break only showed up on a *submodule* import. Fixed by renaming the
   directory (`data_quality/`) rather than fighting Python's import
   resolution. The same class of bug resurfaced with the project's own
   `airflow/` directory, but narrower — nothing under it self-referenced
   `airflow.X`, so the fix there was scoping `pytest.importorskip` to
   `airflow.models` instead of bare `airflow`. **Lesson: before naming a
   project directory after any well-known concept, check whether a pip
   package already owns that name.** (Phases 14, 17)

6. **A general-purpose "does this exist" check needs to handle a missing
   *container*, not just a missing *item*.** `object_exists()` handled a
   missing S3 key but raised on a missing *bucket* — which broke the very
   real "first ever run" case for Gold's SCD2 bootstrap, where nothing has
   been written yet. Fix: treat `NoSuchBucket` the same as `NoSuchKey`.
   (Phase 10)

7. **Unsigned hash output can silently overflow a signed integer type.**
   DuckDB's `hash()` returns `UBIGINT`; PyArrow infers Python ints as
   `int64` by default. A large hash value threw `OverflowError` converting
   into a surrogate key column. Fix: mask off the sign bit before casting
   to `BIGINT` — still a well-distributed key, always in range. (Phase 10)

8. **A query engine may refuse to register a zero-column relation.**
   Building an "empty" table via `schema=pa.schema([])` produces a table
   with zero columns, not zero rows with the right columns — DuckDB
   rejects that outright. An empty dimension (a real case: no products
   ingested yet) then broke any query joining against it. Fix: require an
   explicit schema for the empty case, so the relation still has the right
   column names/types with zero rows. (Phase 10)

9. **Reading the exact same file glob twice in one query (a value and a
   correlated subquery over it) can hit real query-engine bugs, not just
   be inefficient.** `WHERE batch_date = (SELECT MAX(batch_date) FROM
   read_parquet(same_glob))` reliably crashed DuckDB 1.10.1's statistics
   propagator with an internal assertion failure — reproduced independent
   of dbt, directly against `duckdb-python`. Fix: `QUALIFY dense_rank()
   OVER (...) = 1` reads the glob once and produces identical results.
   (Phase 13)

10. **A snapshot/versioning tool's timestamp reflects *when it ran*, not
    any business date embedded in your data.** dbt's `dbt_valid_from`
    carries real wall-clock precision from the actual `dbt snapshot`
    invocation. A point-in-time join that truncated an order's timestamp
    to a bare date before comparing against that full-precision value
    compared "midnight" against a same-day snapshot run and silently
    returned `NULL`. Fix: compare full timestamp precision for that join;
    verified by placing one order between two real, live `dbt snapshot`
    runs and confirming it resolved to the correct historical version, not
    the current one. (Phase 13)

11. **A cross-dialect test engine needs a real translation strategy, not
    an assumption that "it'll probably work."** SQLite has no concept of a
    Postgres-style schema — `metadata.pipeline_runs` fails outright
    (`unknown database metadata`). Fix: SQLAlchemy's `schema_translate_map`
    remaps the schema away at the connection level for non-Postgres
    engines, applied through one shared helper rather than three separate
    reimplementations of the same check (the first version had the fix in
    `create_all()` but not in the query functions, which broke tests that
    queried tables directly instead of going through the client). (Phase 15)

12. **A sensor waiting on an entire run (not one specific task) only
    accepts that run-level state vocabulary.** `ExternalTaskSensor`'s
    `failed_states=["failed", "upstream_failed"]` is invalid when no
    `external_task_id` is given — `upstream_failed` is a per-task concept,
    not a valid `DagRunState`. Fix: `["failed"]` only, when waiting on the
    whole DAG. Caught via a real `DagBag` parse, not by reading the docs
    closely enough beforehand. (Phase 11)

13. **`TaskGroup` prefixes task IDs with the group name** — a test filtering
    on exact `task_id == "cadence_gate"` silently found zero matches
    because the real ID was `source__customers.cadence_gate`. Fix: match
    on suffix. A reminder that testing against the real framework surfaces
    things a mental model of "how it probably works" doesn't. (Phase 17)

14. **Two services in one Docker Compose file can silently claim the same
    host port** if you're not tracking every mapping — `customer-api` and
    `airflow-webserver` both defaulted to host `8080`. Caught by running
    `docker compose config` and grepping the rendered output for the port
    number, not by eyeballing the YAML. (Phase 7)

## Resume bullets (by phase, pick what's relevant)

- Architected a medallion-architecture lakehouse (Bronze/Silver/Gold)
  processing heterogeneous retail data sources via Airflow-orchestrated
  PySpark and dbt pipelines, with automated data quality validation and
  full pipeline observability.
- Built a config-driven ingestion framework (retry-aware HTTP pagination,
  generic DB table extraction, file-glob CSV ingestion) landing
  heterogeneous sources into a uniform format, backed by a mock REST API
  service and unit tests run against a mocked S3 backend.
- Designed a schema-on-read Bronze layer that survives real-world schema
  drift in raw source data — diagnosed and fixed a PyArrow type-unification
  failure by storing raw payloads as JSON rather than eagerly flattening
  into typed columns.
- Built a PySpark Silver layer (casting, deduplication via window
  functions, multi-format date standardization, a broadcast-join
  referential-integrity check) validated against a live local Spark
  session — diagnosed and fixed a Python 3.13/PySpark `toPandas()`
  incompatibility and an ANSI-mode date-parsing regression.
- Implemented a star schema with genuine SCD Type 2 history tracking and
  point-in-time fact-to-dimension joins (both a hand-rolled DuckDB
  implementation and a real dbt snapshot), validated with zero external
  dependencies — diagnosed and fixed 4 real bugs spanning storage-layer
  error handling, integer overflow in surrogate key generation, a DuckDB
  engine-level crash, and a Spark-version-dependent regression.
- Orchestrated a 4-DAG Airflow pipeline (Bronze/Silver/Gold/health-check)
  with cross-DAG sensors, cadence-based task skipping, and a
  Connections-based pre-flight health check — validated via programmatic
  `DagBag` parsing, uncovering a real sensor state-validation bug.
- Integrated Great Expectations for cross-layer data quality validation
  with real HTML report generation — caught and resolved a project/library
  naming collision through direct API verification before it could
  propagate through the codebase.
- Built a lightweight metadata tracking layer (pipeline run history,
  freshness, schema-drift detection, lineage, ownership) with a
  dialect-portable SQLAlchemy client, tested against real SQL execution via
  SQLite rather than mocks.
- Built a pipeline health-check system layered on the metadata store, wired
  into an independently-scheduled Airflow DAG that keeps monitoring even
  when the pipelines it's watching are broken.
- Enforced an 80% test-coverage gate as a hard pytest failure condition
  (91% achieved) across ingestion, transformation, orchestration, data
  quality, and metadata layers.

## What to say if asked "what would you do differently at real scale"

- Bronze's `part-0.parquet` single-file-per-partition and Gold's
  full-table-rebuild both work at this project's data volume but wouldn't
  at real scale — the natural next steps are multi-file Bronze partitions
  and incremental (not full-rebuild) Gold materializations, both already
  called out as deliberate, documented scope boundaries rather than
  oversights (`pipelines/gold/writer.py`, `spark/README.md`).
- The metadata catalog is intentionally lightweight (custom Postgres
  tables, not Hive Metastore/Unity Catalog/OpenMetadata) — the right call
  at this scale, explicitly flagged as a future upgrade once the platform
  outgrows it (`metadata/README.md`).
- Spark I/O goes through boto3 rather than native S3A specifically to avoid
  an untestable `hadoop-aws` version-matching dependency chain — worth
  revisiting once real S3A connectivity can actually be validated against
  a live cluster (`spark/README.md`).
