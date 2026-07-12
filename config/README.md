# config/

All environment and pipeline configuration, kept out of code. No secrets are
ever committed here — real values live in a local `.env` (gitignored),
templated by `.env.example` at the repo root.

- YAML files describe *what* to ingest (source connection refs, table lists,
  file patterns, schedule) — added per source in Phase 7.
- TOML/YAML files describe environment-level settings (MinIO endpoint,
  Postgres hosts, per-environment overrides for local/dev/prod).

Pipeline code reads configuration through a single loader module rather than
scattering `os.environ` calls throughout — added in Phase 3.
