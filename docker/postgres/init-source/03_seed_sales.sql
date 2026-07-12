-- Synthetic seed data for local dev. Deliberately includes some messy rows
-- (nulls, a duplicate-looking email) so Silver-layer cleaning has real work to do.

INSERT INTO sales.stores (store_id, store_name, region, country, opened_date) VALUES
    (1, 'Downtown Flagship', 'Northeast', 'USA', '2015-03-01'),
    (2, 'Westside Mall', 'West', 'USA', '2017-06-15'),
    (3, 'Riverside Outlet', 'South', 'USA', '2019-11-20'),
    (4, 'North Plaza', 'Midwest', 'USA', '2020-02-10'),
    (5, 'Harbor Point', 'East', 'Canada', '2021-09-05')
ON CONFLICT (store_id) DO NOTHING;

INSERT INTO sales.products (product_id, sku, product_name, category, unit_price) VALUES
    (101, 'SKU-101', 'Wireless Mouse', 'Electronics', 24.99),
    (102, 'SKU-102', 'Mechanical Keyboard', 'Electronics', 79.99),
    (103, 'SKU-103', 'USB-C Hub', 'Electronics', 34.50),
    (104, 'SKU-104', 'Standing Desk', 'Furniture', 349.00),
    (105, 'SKU-105', 'Office Chair', 'Furniture', 189.99),
    (106, 'SKU-106', 'Desk Lamp', 'Furniture', 29.95),
    (107, 'SKU-107', 'Notebook Set', 'Stationery', 12.00),
    (108, 'SKU-108', 'Ballpoint Pens (12pk)', 'Stationery', 6.50),
    (109, 'SKU-109', 'Coffee Mug', 'Kitchen', 9.99),
    (110, 'SKU-110', 'Water Bottle', 'Kitchen', 14.99),
    (111, 'SKU-111', 'Backpack', 'Accessories', 59.00),
    (112, 'SKU-112', 'Laptop Sleeve', 'Accessories', 22.50),
    (113, 'SKU-113', 'Bluetooth Speaker', 'Electronics', 45.00),
    (114, 'SKU-114', 'Monitor Stand', 'Furniture', 39.99),
    (115, 'SKU-115', 'Webcam HD', 'Electronics', 54.99)
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO sales.customers (customer_id, email, first_name, last_name, signup_date)
SELECT
    c AS customer_id,
    'customer' || c || '@example.com',
    (ARRAY['Alex','Jordan','Sam','Taylor','Morgan','Casey','Riley','Jamie'])[1 + (c % 8)],
    (ARRAY['Smith','Johnson','Lee','Brown','Garcia','Davis','Miller','Wilson'])[1 + (c % 8)],
    DATE '2019-01-01' + (c * 5)
FROM generate_series(1, 30) AS c
ON CONFLICT (customer_id) DO NOTHING;

-- Deliberately dirty rows: null email, duplicate email across two customer_ids.
INSERT INTO sales.customers (customer_id, email, first_name, last_name, signup_date) VALUES
    (31, NULL, 'Pat', 'Nguyen', '2022-04-01'),
    (32, 'customer5@example.com', 'Dana', 'Kim', '2022-05-11')
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO sales.orders (order_id, customer_id, store_id, order_status, order_ts, updated_ts)
SELECT
    o AS order_id,
    1 + (o % 32),
    1 + (o % 5),
    (ARRAY['completed','completed','completed','pending','cancelled','refunded'])[1 + (o % 6)],
    TIMESTAMP '2024-01-01 08:00:00' + (o || ' hours')::interval,
    TIMESTAMP '2024-01-01 08:00:00' + (o || ' hours')::interval + INTERVAL '10 minutes'
FROM generate_series(1, 60) AS o
ON CONFLICT (order_id) DO NOTHING;

INSERT INTO sales.order_lines (order_line_id, order_id, product_id, quantity, unit_price, discount_pct)
SELECT
    ol AS order_line_id,
    1 + (ol % 60),
    101 + (ol % 15),
    1 + (ol % 4),
    (SELECT unit_price FROM sales.products WHERE product_id = 101 + (ol % 15)),
    CASE WHEN ol % 10 = 0 THEN 15.0 ELSE 0 END
FROM generate_series(1, 120) AS ol
ON CONFLICT (order_line_id) DO NOTHING;
