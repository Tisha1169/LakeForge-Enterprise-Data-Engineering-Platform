INSERT INTO inventory.suppliers (supplier_id, supplier_name, country, lead_time_days) VALUES
    (1, 'Global Office Supply Co', 'USA', 7),
    (2, 'TechParts Direct', 'China', 21),
    (3, 'Nordic Furnishings', 'Sweden', 14),
    (4, 'Pacific Trading Ltd', 'Vietnam', 18)
ON CONFLICT (supplier_id) DO NOTHING;

-- 3 days of snapshots x 5 stores x 15 products = 225 rows.
INSERT INTO inventory.stock_snapshots
    (snapshot_id, store_id, product_id, snapshot_date, quantity_on_hand, reorder_point, supplier_id)
SELECT
    (d * 1000) + (s * 100) + p AS snapshot_id,
    s AS store_id,
    100 + p AS product_id,
    DATE '2024-01-01' + d AS snapshot_date,
    20 + ((s + p + d) % 80) AS quantity_on_hand,
    10,
    1 + ((s + p) % 4)
FROM generate_series(0, 2) AS d
CROSS JOIN generate_series(1, 5) AS s
CROSS JOIN generate_series(1, 15) AS p
ON CONFLICT (store_id, product_id, snapshot_date) DO NOTHING;
