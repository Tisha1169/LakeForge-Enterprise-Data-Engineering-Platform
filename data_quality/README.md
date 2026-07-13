# data_quality/

Great Expectations validation suites, run against Bronze and Silver tables.

**Named `data_quality/`, not `great_expectations/`** — the folder name
originally matched the master-prompt's suggested layout, but a directory
literally named `great_expectations` sitting on `sys.path` collides with the
`great_expectations` pip package itself. `import great_expectations` still
correctly resolves to the real library either way (Python prefers a regular
package over a same-named namespace-package directory), but any of *our own*
submodules — `from great_expectations.context import get_context` — silently
resolved to "look inside the pip package for a `context` module," which
doesn't exist, and raised `ModuleNotFoundError`. Reproduced directly with the
installed library before writing more code around it, then renamed the
directory rather than working around the shadowing.

- `context.py` — `get_context()`: a file-based GX context (not ephemeral) so
  Data Docs (HTML reports) actually get written to disk, under
  `data_quality/gx/uncommitted/data_docs/` (gitignored, regenerated per run —
  `data_quality/gx/` itself is entirely GX-managed generated state; our
  `suites.py` is the real source of truth, not GX's persisted YAML).
- `suites.py` — one `build_*_suite()` function per table:
  - `bronze_customers_suite` — schema-only (`ExpectTableColumnsToMatchSet`):
    Bronze is deliberately schema-on-read (Phase 8), so null/uniqueness
    checks don't belong here — only "did the expected columns show up at
    all" (schema drift).
  - `silver_customers_suite` — not-null + unique `customer_id`,
    `loyalty_tier` in the known set, `email` matches a basic regex.
  - `silver_orders_suite` — not-null + unique `order_id`, `order_status` in
    the known set.
  - `silver_order_lines_suite(valid_order_ids)` — **referential integrity**:
    every `order_id` must exist in Silver `orders` for the same batch. Takes
    the valid-ID set as a parameter since it's only known at validation
    time (computed fresh from a real read of Silver orders), not at suite-
    definition time — a static suite can't express this.
  - `silver_products_suite` / `silver_stores_suite` — not-null + unique
    natural keys.
- `runner.py` — `validate_bronze_customers`/`validate_silver_*(batch_date)`.
  Reads through `pipelines.bronze`/`pipelines.silver` (the same tested boto3
  boundary every other layer uses — see `spark/README.md` for the same
  reasoning applied to Spark) rather than pointing GX at S3 directly, builds
  a pandas DataFrame, runs a GX Checkpoint, and returns a `QualityResult`
  (success, expectation counts, row count). Each call generates a uniquely-
  named data source/asset/batch-definition/checkpoint (a UUID suffix) rather
  than trying to reuse fixed names across runs — sidesteps GX's
  already-exists conflicts entirely instead of reconciling them.

A failed expectation suite is meant to quarantine the batch rather than
silently letting bad data flow into Silver/Gold — the actual quarantine
behavior (what happens when `QualityResult.success` is `False`) is wired
into Airflow as a follow-up, not yet implemented here.
