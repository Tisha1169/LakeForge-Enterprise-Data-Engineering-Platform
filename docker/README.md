# docker/

Containerization for every service in the platform. The goal: `docker compose
up` brings up the entire stack — Postgres, MinIO, Airflow, Spark — with no
manual setup steps.

- `postgres/init-source/` — SQL run on first boot of `postgres-source`:
  extensions, the `sales` schema (stores, products, customers, orders,
  order_lines) and `inventory` schema (stock_snapshots, suppliers), plus
  synthetic seed data (deliberately includes some nulls/dirty rows for
  Silver-layer cleaning to handle later).
- `postgres/init-warehouse/` — SQL run on first boot of `postgres-warehouse`:
  extensions, the `gold` schema (reserved — dbt/`pipelines/gold` write
  Parquet to MinIO, not Postgres, so no tables land here), and the
  `metadata` schema (`pipeline_runs`, `table_freshness`, `schema_versions`,
  `table_ownership`, `lineage` — see `metadata/README.md`), seeded with
  static ownership/lineage reference data.
- `minio/init-buckets.sh` — idempotent script (run by the `minio-init`
  one-shot container) that creates the landing/bronze/silver/gold buckets.
- `airflow/Dockerfile` — extends the official Airflow image with the
  dependencies our DAGs/pipeline code need. Pinned to **Python 3.12**
  (Airflow's own constraint) — independent of the 3.13 used elsewhere in the
  platform; DAGs only import pipeline code, they don't need a shared
  interpreter version.
- `spark/Dockerfile` — extends the Bitnami Spark image with the Python deps
  our PySpark jobs need; used for both `spark-master` and `spark-worker`.
- `customer-api/` — a small FastAPI service simulating the Customer REST API
  for local dev: paginated `/customers`, seeded from
  `sample_data/customers/customers.json`. Exists so ingestion genuinely
  exercises HTTP pagination/retry against something that behaves like a real
  third-party API, not a local file read.

## Services (root `docker-compose.yml`)

| Service | Purpose |
|---|---|
| `postgres-source` | Simulated Sales + Inventory operational DBs (host port 5432) |
| `postgres-warehouse` | Warehouse + metadata schema (host port 5433) |
| `postgres-airflow` | Airflow's internal metadata DB — kept separate from business data |
| `minio` / `minio-init` | Object storage (console on host port 9001) + bucket bootstrap |
| `customer-api` | Mock Customer REST API (host port 8000, container port 8080) |
| `spark-master` / `spark-worker` | Local Spark cluster for PySpark jobs |
| `airflow-init` / `airflow-webserver` / `airflow-scheduler` | LocalExecutor Airflow (UI on host port 8080) |

## Running it

```bash
cp .env.example .env   # first time only
docker compose up -d
```

Airflow UI: http://localhost:8080 (default `admin`/`admin`, from `.env`)
MinIO console: http://localhost:9001
