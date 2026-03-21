-- Dust quickstart: run with `dust query -f examples/quickstart.sql`
--
-- Product inventory example. Matches the flow in docs/quickstart.md.

-- Schema
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY,
    sku TEXT NOT NULL,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    unit_price_cents INTEGER NOT NULL,
    stock INTEGER NOT NULL
);

-- Sample data
INSERT INTO products (id, sku, name, category, unit_price_cents, stock) VALUES
    (1, 'WDG-001', 'Standard Widget',    'widgets',    1499, 340),
    (2, 'WDG-002', 'Premium Widget',     'widgets',    2999, 125),
    (3, 'BLT-001', 'M6 Bolt',            'fasteners',  29,   8400),
    (4, 'BLT-002', 'M8 Bolt',            'fasteners',  45,   6200),
    (5, 'GDG-001', 'USB-C Hub',          'gadgets',    3495, 58),
    (6, 'GDG-002', 'Bluetooth Dongle',   'gadgets',    1299, 210),
    (7, 'GDG-003', 'Portable SSD 1TB',   'gadgets',    8999, 42);

-- All products sorted by price descending
SELECT sku, name, unit_price_cents, stock FROM products ORDER BY unit_price_cents DESC;

-- Category summary: count and total stock per category
SELECT category, count(*), sum(stock) FROM products GROUP BY category;

-- Gadgets over $20 (2000 cents)
SELECT sku, name, unit_price_cents FROM products
    WHERE category = 'gadgets' AND unit_price_cents > 2000
    ORDER BY unit_price_cents DESC;

-- Low-stock items (under 100 units)
SELECT sku, name, stock FROM products WHERE stock < 100 ORDER BY stock;

-- Simulated price increase on gadgets
UPDATE products SET unit_price_cents = unit_price_cents + 500 WHERE category = 'gadgets';

-- Verify the update
SELECT sku, name, unit_price_cents FROM products WHERE category = 'gadgets' ORDER BY sku;

-- Remove high-stock commodity items
DELETE FROM products WHERE stock > 5000;

-- Final state
SELECT * FROM products ORDER BY id;
