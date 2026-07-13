# Deployment Guide

OpenLake runs entirely locally via `docker compose up -d` (see the root
[README](../README.md) and [docker/README.md](../docker/README.md)). This
guide covers moving it to a real cloud environment — AWS is used as the
concrete example since it maps most directly onto the local stack, with
Azure/GCP equivalents noted per component. Every component was chosen in
Phase 1 specifically so this migration is **configuration, not rewrite** —
see `docs/architecture.md` §4 for the original rationale.

## Component-by-component migration

| Local component | AWS target | Azure | GCP | Code change needed |
|---|---|---|---|---|
| MinIO | S3 | Azure Blob Storage | Cloud Storage | **None.** `pipelines/storage.py` already speaks the S3 API; set `MINIO_ENDPOINT` to `s3.amazonaws.com` (or leave unset — boto3 defaults there), swap `MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` for real IAM credentials (or, better, an IAM role — see below), set `MINIO_SECURE=true`. |
| `postgres-source`, `postgres-warehouse` | RDS for PostgreSQL | Azure Database for PostgreSQL | Cloud SQL for PostgreSQL | **None.** Point `SOURCE_DB_HOST`/`WAREHOUSE_DB_HOST` at the managed instance's endpoint. |
| Airflow (LocalExecutor, self-hosted) | MWAA (Managed Workflows for Apache Airflow) | Azure Data Factory's Airflow integration, or self-hosted on AKS | Cloud Composer | DAG code is unchanged (`airflow/dags/`); `docker/airflow/Dockerfile`'s dependency list becomes MWAA's `requirements.txt`. LocalExecutor doesn't scale past one machine — MWAA runs CeleryExecutor/KubernetesExecutor, which is a config change (`AIRFLOW__CORE__EXECUTOR`), not a DAG change. |
| Spark (local cluster) | EMR (or EMR Serverless) | Azure Synapse / HDInsight | Dataproc | `spark/jobs/common.py`'s `SPARK_MASTER_URL` points at the EMR master instead of `spark://spark-master:7077`. If real S3A reads become worth it at cloud scale (see `spark/README.md`), this is also where that native-connector work would land. |
| `customer-api` (mock) | *(retired)* | — | — | This service only exists to simulate a third-party API for local dev — in a real deployment, `CUSTOMER_API_BASE_URL` points at the actual vendor's API. |
| dbt (dbt-duckdb) | dbt against Snowflake/BigQuery/Redshift, **or** dbt-duckdb reading S3 directly | — | — | If staying with DuckDB: `OPENLAKE_SILVER_BASE` already points at `s3://openlake-silver` by default (see `dbt/models/staging/_sources.yml`) — this already works against real S3 with real credentials, no code change. Migrating to a cloud warehouse instead is a `profiles.yml` target change + `dbt-snowflake`/`dbt-bigquery` adapter swap. |

## Secrets

Local dev uses a `.env` file (gitignored, never committed — see
`.env.example`). **Do not carry that pattern into production.** Use:

- AWS: Secrets Manager or Parameter Store, injected as environment variables
  via ECS task definitions / MWAA's `secretsBackend` config / EKS External
  Secrets Operator.
- Azure: Key Vault.
- GCP: Secret Manager.

Prefer IAM roles over long-lived access keys wherever the runtime supports
it (ECS task roles, EKS IRSA, MWAA's execution role) — `pipelines/storage.py`
builds its boto3 client from `settings.minio_access_key`/`minio_secret_key`,
so the config-loader change is: leave those unset and let boto3's default
credential chain (instance metadata / IRSA) take over.

## Networking

Local Docker Compose uses a single bridge network with service-name DNS
(`postgres-source`, `minio`, etc.). In a real deployment:

- Put `postgres-source`/`postgres-warehouse` (RDS) and Spark/Airflow compute
  in private subnets; only the Airflow webserver UI needs any public
  exposure (behind a load balancer + SSO, not directly).
- S3 access from Airflow/Spark should go through a VPC endpoint, not the
  public internet, once you're off MinIO.

## Migration checklist

1. Provision RDS (source + warehouse) and S3 buckets (landing/bronze/
   silver/gold) matching `docker/postgres/init-*` and
   `docker/minio/init-buckets.sh`'s bucket names/lifecycle rules.
2. Run the `docker/postgres/init-*/*.sql` scripts against the real RDS
   instances (they were designed to be idempotent — `IF NOT EXISTS`
   throughout).
3. Set every `.env` variable via the target platform's secrets mechanism
   instead of a file.
4. Deploy the Airflow image (`docker/airflow/Dockerfile`) to MWAA/Composer/
   self-hosted; point its DAGs folder at `airflow/dags/`.
5. Point Spark jobs at the real cluster's master URL.
6. Update `OPENLAKE_SILVER_BASE` (dbt) and any hardcoded `s3://` defaults to
   the real bucket names if they differ from the local defaults
   (`openlake-landing`/`-bronze`/`-silver`/`-gold`).
7. Retire `customer-api`; point `CUSTOMER_API_BASE_URL` at the real vendor.
8. Run the full pipeline once against the new environment before cutting
   over the schedule — `docker compose`'s local run is exactly the
   integration test for this, since every component speaks the same API
   locally and in the cloud.

## What doesn't change

Every table's schema, every transformation's logic, every test — none of
this is cloud-specific. The entire point of building on S3-API object
storage, standard PostgreSQL, and open-source Airflow/Spark/dbt rather than
a specific cloud vendor's proprietary services is that this list is short.
