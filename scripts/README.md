# scripts/

Local-dev tooling that doesn't belong in `pipelines/` (not part of any
pipeline's runtime path).

- `generate_dbt_local_fixtures.py` — writes small Parquet fixtures matching
  Silver's exact on-disk layout (`source/table/batch_date=<date>/part-0.parquet`),
  so the dbt project (`dbt/`) can be run and tested against a local directory
  instead of real MinIO/S3. See `dbt/README.md` for usage.
