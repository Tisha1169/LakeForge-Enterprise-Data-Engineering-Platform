-- Simulated OLTP schema for the Sales system.
-- Grain: sales_order_line = one row per line item on an order.
CREATE SCHEMA IF NOT EXISTS sales;

CREATE TABLE IF NOT EXISTS sales.stores (
    store_id        INTEGER PRIMARY KEY,
    store_name      TEXT NOT NULL,
    region          TEXT NOT NULL,
    country         TEXT NOT NULL,
    opened_date     DATE
);

CREATE TABLE IF NOT EXISTS sales.products (
    product_id      INTEGER PRIMARY KEY,
    sku             TEXT NOT NULL,
    product_name    TEXT NOT NULL,
    category        TEXT,
    unit_price      NUMERIC(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS sales.customers (
    customer_id     INTEGER PRIMARY KEY,
    email           TEXT,
    first_name      TEXT,
    last_name       TEXT,
    signup_date     DATE
);

-- Order header. status is mutable in the real OLTP system (updated in place),
-- which is exactly why Bronze snapshots + Silver dedup/versioning matter.
CREATE TABLE IF NOT EXISTS sales.orders (
    order_id        INTEGER PRIMARY KEY,
    customer_id     INTEGER REFERENCES sales.customers(customer_id),
    store_id        INTEGER REFERENCES sales.stores(store_id),
    order_status    TEXT NOT NULL,          -- pending | completed | cancelled | refunded
    order_ts        TIMESTAMP NOT NULL,
    updated_ts      TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS sales.order_lines (
    order_line_id   INTEGER PRIMARY KEY,
    order_id        INTEGER REFERENCES sales.orders(order_id),
    product_id      INTEGER REFERENCES sales.products(product_id),
    quantity        INTEGER NOT NULL,
    unit_price      NUMERIC(10, 2) NOT NULL,
    discount_pct    NUMERIC(5, 2) DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_orders_updated_ts ON sales.orders(updated_ts);
CREATE INDEX IF NOT EXISTS idx_order_lines_order_id ON sales.order_lines(order_id);
