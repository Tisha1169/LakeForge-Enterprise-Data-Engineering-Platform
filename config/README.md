# config/

All environment and pipeline configuration, kept out of code. No secrets are
ever committed here — real values live in a local `.env` (gitignored),
templated by `.env.example` at the repo root.

- `settings.py` — the single loader. A `pydantic-settings` `Settings` class
  reads environment variables / `.env` and exposes them as a typed `settings`
  object (`from config.settings import settings`). No module anywhere else in
  the codebase calls `os.environ` directly.
- `sources.py` — typed loader (`SourceConfig` pydantic model) for the YAML
  files below. Ingestion code (`pipelines/ingestion/`) is generic over this
  model, never over a specific source.
- `sources/` — one YAML file per data source: `customers.yaml` (API),
  `sales.yaml` / `inventory.yaml` (DB tables), `suppliers.yaml` (file drop).
  Adding a new source is "add a YAML file," not "edit Python."

See [.env.example](../.env.example) at the repo root for every environment
variable `settings.py` reads, with local-dev defaults documented inline.
