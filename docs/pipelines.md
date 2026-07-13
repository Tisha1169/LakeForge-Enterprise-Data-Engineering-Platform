# Pipeline Documentation

A "follow the data" reference — which DAG runs what, in what order, calling
which code. For *why* things are built this way, see
[architecture.md](architecture.md); for table/column detail, see
[data_dictionary.md](data_dictionary.md); for per-layer implementation
detail, see each layer's own `README.md` (linked below).

## End-to-end flow for one batch_date

```
openlake_bronze_ingestion (@daily)
  preflight_check_source_db                          [PostgresHook connectivity check]
  -> per source (customers, sales, sales_order_lines,
     sales_products, sales_stores, inventory, suppliers):
       cadence_gate     -- pipelines/ingestion/, config/sources/*.yaml
       -> ingest        -- BaseIngestion.run() -> lands NDJSON in MinIO
       -> to_bronze      -- pipelines/bronze/writer.py -> Parquet, payload column

openlake_silver_transform (@daily, waits for the above via ExternalTaskSensor)
  -> per Silver job (parallel, except sales_order_lines waits for sales_orders):
       silver__<name>   -- spark/jobs/<name>_silver.py -> cleaned, cast, deduped Parquet

openlake_gold_build (@daily, waits for the above)
  build_gold             -- pipelines/gold/runner.py -> dim_date, dim_customer (SCD2),
                             dim_product, dim_store, fact_sales, daily_sales_summary

openlake_health_check (@daily, independent — no sensor)
  check_platform_health  -- monitoring/health.py against metadata/ tables
```

`dbt/` (Phase 13) is a second, parallel implementation of the Gold step —
run manually (`dbt run`) or as a future Airflow task, not currently wired
into `openlake_gold_build`. See `dbt/README.md`.

## Where the actual logic lives, per layer

| Layer | Orchestration (Airflow) | Logic |
|---|---|---|
| Ingestion | `airflow/dags/bronze_ingestion_dag.py` | `pipelines/ingestion/` — see [pipelines/README.md](../pipelines/README.md) |
| Bronze | (same DAG, `to_bronze` task) | `pipelines/bronze/writer.py` |
| Silver | `airflow/dags/silver_transform_dag.py` | `spark/jobs/*_silver.py` — see [spark/README.md](../spark/README.md) |
| Gold | `airflow/dags/gold_build_dag.py` | `pipelines/gold/` (Python/DuckDB) and `dbt/models/marts/` (dbt) |
| Data quality | not yet wired into a DAG (opt-in, see `data_quality/README.md`) | `data_quality/runner.py` |
| Metadata | called from within Bronze/ingestion/Gold tasks (opt-in `metadata_engine` param) | `metadata/client.py` |
| Monitoring | `airflow/dags/health_check_dag.py` | `monitoring/health.py` |

## Tracing a specific table's lineage

`metadata.lineage` (seeded in `docker/postgres/init-warehouse/02_schema_metadata.sql`)
records the structural upstream/downstream edges between tables — query it
via `metadata.client.get_lineage(engine, layer, table_name)`, or read the
seed SQL directly for the full static graph. Per-run lineage (which specific
run produced which output, for a specific `batch_date`) lives in
`metadata.pipeline_runs`, correlated by `batch_date` across layers.
