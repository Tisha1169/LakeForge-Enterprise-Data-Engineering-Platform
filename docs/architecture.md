# OpenLake — Architecture

## 1. Business context

OpenLake simulates the internal data platform of a multinational retail company
("OpenLake Retail Co."). The platform ingests data from four heterogeneous source
systems and transforms it into trusted, analytics-ready datasets.

| Source | Type | Format | Update pattern |
|---|---|---|---|
| Customer API | REST API | JSON | Daily pull, append-only new/updated customers |
| Sales System | Operational DB (Postgres) | Relational rows | Transactional, daily incremental |
| Inventory DB | Operational DB (Postgres) | Relational rows | Daily snapshot |
| Supplier Data | File drop | CSV | Weekly batch |

## 2. Medallion architecture

```
Raw Sources -> Ingestion -> Landing (MinIO) -> Bronze (Parquet, immutable)
  -> Validation (Great Expectations) -> Silver (clean, deduped, PySpark)
  -> Business Transformation (dbt) -> Gold (star schema)
  -> Warehouse (Postgres/DuckDB) -> Analytics Consumers (Superset/BI)
```

Cross-cutting concerns that touch every layer: **Airflow orchestration**,
**custom metadata tracking**, **Great Expectations data quality**, and
**structured logging/monitoring**.

### Why medallion, not straight-to-warehouse?

- **Replayability** — bronze is immutable and never transformed in place, so any
  bug in silver/gold logic can be fixed and the layer reprocessed from bronze
  without re-ingesting from source systems (which may not even have the data
  anymore, e.g. an API with no historical replay).
- **Blast radius isolation** — a broken transformation corrupts silver/gold, not
  the raw historical record.
- **Different consumers, different layers** — data scientists / ad hoc analysis
  can read silver directly; BI dashboards read only gold.

## 3. Key design decisions

| Decision | Choice | Rationale |
|---|---|---|
| Storage format | Parquet everywhere | Columnar, compressed, splittable; native to Spark/DuckDB/dbt |
| Object storage | MinIO (S3-compatible) | Code written against the S3 API works unchanged against real AWS S3 later |
| Bronze immutability | Append-only, never update/delete | Full audit trail, replay capability |
| Orchestrator | Apache Airflow | Industry-standard DAG scheduling and dependency management |
| Bronze -> Silver | PySpark | Row-level cleaning/dedup/normalization is engineering-heavy, benefits from a general-purpose distributed engine |
| Silver -> Gold | Both: hand-rolled DuckDB SQL (`pipelines/gold/`, Phase 10) AND a real dbt project (`dbt/`, Phase 13) — same star schema, two implementations | Phase 10 needed something testable with zero external services (DuckDB is embedded, no JVM/Docker) before a dbt setup existed. Phase 13 formalizes the same tables as actual dbt models/sources/snapshots/tests — dbt's `snapshot` mechanism replaces the Python SCD2 implementation with the real, standard tool for it. Both are genuinely tested (Phase 10: 18 pytest cases; Phase 13: real `dbt run`/`dbt test`/two live `dbt snapshot` runs against local Parquet fixtures) |
| Warehouse | Postgres + DuckDB | Postgres for transactional-shaped queries and metadata; DuckDB for fast local OLAP on gold Parquet — zero cloud cost, patterns transfer directly to Snowflake/BigQuery/Redshift |
| Metadata layer | Custom lightweight Postgres tables | Demonstrates understanding of lineage/freshness/ownership tracking without the operational overhead of a full catalog (Hive Metastore / Unity Catalog) — noted as a future upgrade |
| Advanced table formats (Delta/Iceberg) | Deferred, optional advanced phase | ACID-on-object-storage adds real complexity; introduced only once the core medallion flow is solid |

## 4. Cloud portability

Every component is chosen so a future migration to AWS/Azure/GCP is a
**configuration change, not a rewrite**:

- MinIO -> S3 / Azure Blob / GCS (same S3-compatible client code)
- Airflow -> MWAA / Composer / Azure equivalents (same DAGs)
- Postgres -> RDS / Cloud SQL / Azure Database for PostgreSQL
- dbt -> unchanged (dbt already targets cloud warehouses natively)

## 5. Repository layout

See the top-level [README.md](../README.md) for the full folder structure and
what belongs in each directory.
