# OpenLake

**Open Source Enterprise Data Lakehouse Platform**

OpenLake is a local-first, Dockerized implementation of a medallion-architecture
data lakehouse, modeled on the internal data platform of a multinational retail
company. It ingests data from heterogeneous source systems (REST API, relational
databases, CSV files), and produces trusted, analytics-ready datasets through a
Bronze -> Silver -> Gold pipeline — orchestrated by Airflow, transformed with
PySpark and dbt, validated with Great Expectations, and fully observable through
a custom metadata and logging layer.

> Status: under active build-out, phase by phase. See [docs/architecture.md](docs/architecture.md)
> for full design rationale.

## Architecture

```
Raw Sources -> Ingestion -> Landing (MinIO) -> Bronze (Parquet, immutable)
  -> Validation (Great Expectations) -> Silver (clean, deduped, PySpark)
  -> Business Transformation (dbt) -> Gold (star schema)
  -> Warehouse (Postgres/DuckDB) -> Analytics Consumers (Superset/BI)
```

Cross-cutting: Airflow orchestration, custom metadata tracking, data quality
validation, structured logging/monitoring.

Full rationale for every decision (why Parquet, why MinIO, why PySpark for
Silver but dbt for Gold, etc.) is in [docs/architecture.md](docs/architecture.md).

## Tech stack

| Layer | Tools |
|---|---|
| Language | Python 3.13, SQL |
| Orchestration | Apache Airflow |
| Transformation | PySpark, dbt Core |
| Storage | MinIO (S3-compatible), Parquet |
| Databases | PostgreSQL, DuckDB |
| Data quality | Great Expectations |
| Containerization | Docker, Docker Compose |
| CI/CD | GitHub Actions |
| Testing | pytest |
| Visualization (optional) | Apache Superset |

## Repository structure

```
openlake/
├── airflow/              # DAGs, plugins, Airflow config
├── docker/                # Dockerfiles + compose configs per service
├── dbt/                   # dbt project — Silver -> Gold business logic
├── spark/                 # PySpark jobs — Bronze -> Silver transformations
├── pipelines/
│   ├── ingestion/          # Per-source ingestion modules (api/, db/, files/)
│   ├── bronze/             # Raw immutable landing logic
│   ├── silver/             # Silver job orchestration
│   └── gold/               # Gold star schema (Python/DuckDB build)
├── data_quality/          # Great Expectations suites (not "great_expectations/" — collides with the pip package name)
├── metadata/              # Pipeline run / freshness / lineage tracking
├── monitoring/            # Logging config, health checks
├── config/                # YAML/TOML configuration (no secrets)
├── scripts/               # Local-dev tooling (e.g. dbt fixture generator)
├── tests/                 # pytest suite, mirrors pipelines/
├── sample_data/           # Synthetic seed data for local dev
├── docs/                  # Architecture, developer, deployment, interview docs
└── .github/workflows/     # CI/CD
```

Every folder has its own `README.md` explaining its purpose in more depth.

## Getting started

```bash
cp .env.example .env
docker compose up -d
```

This brings up Postgres (source + warehouse), MinIO, a local Spark cluster,
and Airflow (LocalExecutor). See [docker/README.md](docker/README.md) for the
full service list and ports, and [docs/developer_guide.md](docs/developer_guide.md)
for Python environment setup (`uv`) used for local (non-Docker) development.

- Airflow UI: http://localhost:8080
- MinIO console: http://localhost:9001

Sample-data seeding and full source-system schemas land in Phase 5.

## Project status

Built in 20 phases (see [docs/architecture.md](docs/architecture.md) for the
full roadmap), from architecture through ingestion, Bronze/Silver/Gold,
orchestration, data quality, metadata, testing, documentation, and CI/CD.

## License

MIT (or update as preferred).
