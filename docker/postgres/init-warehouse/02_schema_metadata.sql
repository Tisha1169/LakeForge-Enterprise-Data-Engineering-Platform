-- Metadata schema: pipeline run history, table freshness, schema version
-- history, ownership, and lineage. Reserved by 01_schemas.sql (Phase 5),
-- populated here (Phase 15). Mirrored by metadata/schema.py's SQLAlchemy
-- Core Table definitions — kept manually in sync (no Alembic yet; a real
-- next step once this schema needs to evolve under active use).

CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_name   TEXT NOT NULL,
    layer           TEXT NOT NULL,          -- ingestion | bronze | silver | gold | quality
    source_name     TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    batch_date      DATE NOT NULL,
    status          TEXT NOT NULL,          -- running | success | failed
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    row_count       BIGINT,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_table_batch
    ON metadata.pipeline_runs (layer, table_name, batch_date);

CREATE TABLE IF NOT EXISTS metadata.table_freshness (
    layer                   TEXT NOT NULL,
    table_name              TEXT NOT NULL,
    last_successful_batch_date DATE NOT NULL,
    last_updated_at         TIMESTAMPTZ NOT NULL,
    last_row_count          BIGINT,
    PRIMARY KEY (layer, table_name)
);

CREATE TABLE IF NOT EXISTS metadata.schema_versions (
    id              SERIAL PRIMARY KEY,
    layer           TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    columns         JSONB NOT NULL,          -- sorted list of column names, the version's fingerprint
    detected_at     TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_schema_versions_table
    ON metadata.schema_versions (layer, table_name, detected_at);

CREATE TABLE IF NOT EXISTS metadata.table_ownership (
    layer           TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    owner_team      TEXT NOT NULL,
    owner_contact   TEXT,
    description     TEXT,
    PRIMARY KEY (layer, table_name)
);

CREATE TABLE IF NOT EXISTS metadata.lineage (
    layer                   TEXT NOT NULL,
    table_name              TEXT NOT NULL,
    upstream_layer          TEXT NOT NULL,
    upstream_table_name     TEXT NOT NULL,
    PRIMARY KEY (layer, table_name, upstream_layer, upstream_table_name)
);

-- Static ownership seed data — the data platform team owns everything at
-- this project's scale; a larger org would split this by domain team.
INSERT INTO metadata.table_ownership (layer, table_name, owner_team, owner_contact, description) VALUES
    ('bronze', 'customers', 'data-platform', 'data-platform@openlake.local', 'Raw customer records from the Customer API'),
    ('silver', 'customers', 'data-platform', 'data-platform@openlake.local', 'Cleaned customer master'),
    ('silver', 'orders', 'data-platform', 'data-platform@openlake.local', 'Cleaned sales orders'),
    ('silver', 'order_lines', 'data-platform', 'data-platform@openlake.local', 'Cleaned order line items'),
    ('silver', 'products', 'data-platform', 'data-platform@openlake.local', 'Cleaned product catalog'),
    ('silver', 'stores', 'data-platform', 'data-platform@openlake.local', 'Cleaned store master'),
    ('gold', 'dim_customer', 'data-platform', 'data-platform@openlake.local', 'SCD2 customer dimension'),
    ('gold', 'fact_sales', 'data-platform', 'data-platform@openlake.local', 'Sales fact table, grain: order line')
ON CONFLICT (layer, table_name) DO NOTHING;

-- Lineage seed data — the fixed structural edges of the medallion pipeline
-- (Bronze -> Silver -> Gold table dependencies). Pipeline-run-level lineage
-- (which specific run produced which output) lives in pipeline_runs itself,
-- correlated by batch_date.
INSERT INTO metadata.lineage (layer, table_name, upstream_layer, upstream_table_name) VALUES
    ('silver', 'customers', 'bronze', 'customers'),
    ('silver', 'orders', 'bronze', 'orders'),
    ('silver', 'order_lines', 'bronze', 'order_lines'),
    ('silver', 'order_lines', 'silver', 'orders'),  -- Phase 12's broadcast-join dependency
    ('gold', 'dim_customer', 'silver', 'customers'),
    ('gold', 'fact_sales', 'silver', 'order_lines'),
    ('gold', 'fact_sales', 'silver', 'orders'),
    ('gold', 'fact_sales', 'gold', 'dim_customer'),
    ('gold', 'fact_sales', 'gold', 'dim_product'),
    ('gold', 'fact_sales', 'gold', 'dim_store')
ON CONFLICT DO NOTHING;
