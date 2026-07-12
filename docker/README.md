# docker/

Containerization for every service in the platform. The goal: `docker compose
up` brings up the entire stack — Postgres, MinIO, Airflow, Spark — with no
manual setup steps.

- `postgres/init-source/` — SQL run on first boot of `postgres-source`
  (extensions now; full source-system schema + seed data in Phase 5).
- `postgres/init-warehouse/` — SQL run on first boot of `postgres-warehouse`
  (extensions now; warehouse + metadata schema in Phase 5 / Phase 15).
- `minio/init-buckets.sh` — idempotent script (run by the `minio-init`
  one-shot container) that creates the landing/bronze/silver/gold buckets.
- `airflow/Dockerfile` — extends the official Airflow image with the
  dependencies our DAGs/pipeline code need. Pinned to **Python 3.12**
  (Airflow's own constraint) — independent of the 3.13 used elsewhere in the
  platform; DAGs only import pipeline code, they don't need a shared
  interpreter version.
- `spark/Dockerfile` — extends the Bitnami Spark image with the Python deps
  our PySpark jobs need; used for both `spark-master` and `spark-worker`.

## Services (root `docker-compose.yml`)

| Service | Purpose |
|---|---|
| `postgres-source` | Simulated Sales + Inventory operational DBs (host port 5432) |
| `postgres-warehouse` | Warehouse + metadata schema (host port 5433) |
| `postgres-airflow` | Airflow's internal metadata DB — kept separate from business data |
| `minio` / `minio-init` | Object storage (console on host port 9001) + bucket bootstrap |
| `spark-master` / `spark-worker` | Local Spark cluster for PySpark jobs |
| `airflow-init` / `airflow-webserver` / `airflow-scheduler` | LocalExecutor Airflow (UI on host port 8080) |

## Running it

```bash
cp .env.example .env   # first time only
docker compose up -d
```

Airflow UI: http://localhost:8080 (default `admin`/`admin`, from `.env`)
MinIO console: http://localhost:9001
