# docker/

Containerization for every service in the platform. The goal: `docker compose
up` brings up the entire stack — Postgres, MinIO, Airflow, Spark — with no
manual setup steps.

- `postgres/` — init SQL scripts (source system schemas, warehouse schema,
  metadata schema) mounted into the Postgres container on first boot.
- `minio/` — bucket bootstrap scripts/policies for the landing/bronze/silver/
  gold buckets.
- `airflow/` — Dockerfile extending the official Airflow image with our
  Python dependencies and DAG/plugin mounts.
- `spark/` — Dockerfile for the PySpark job runner image.

The root-level `docker-compose.yml` (added in Phase 4) ties these together.
