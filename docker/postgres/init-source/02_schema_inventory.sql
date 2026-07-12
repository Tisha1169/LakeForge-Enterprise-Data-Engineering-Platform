-- Simulated inventory system: daily snapshot grain (store_id, product_id, snapshot_date).
CREATE SCHEMA IF NOT EXISTS inventory;

CREATE TABLE IF NOT EXISTS inventory.stock_snapshots (
    snapshot_id     BIGINT PRIMARY KEY,
    store_id        INTEGER NOT NULL,
    product_id      INTEGER NOT NULL,
    snapshot_date   DATE NOT NULL,
    quantity_on_hand INTEGER NOT NULL,
    reorder_point   INTEGER,
    supplier_id     INTEGER
);

CREATE TABLE IF NOT EXISTS inventory.suppliers (
    supplier_id     INTEGER PRIMARY KEY,
    supplier_name   TEXT NOT NULL,
    country         TEXT,
    lead_time_days  INTEGER
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_stock_snapshot_grain
    ON inventory.stock_snapshots(store_id, product_id, snapshot_date);
