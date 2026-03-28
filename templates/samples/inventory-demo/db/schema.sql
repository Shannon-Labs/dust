CREATE TABLE suppliers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    lead_time_days INTEGER NOT NULL
);

CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    sku TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    stock INTEGER NOT NULL,
    reorder_point INTEGER NOT NULL,
    supplier_id INTEGER NOT NULL
);

CREATE TABLE purchase_orders (
    id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    ordered_on TEXT NOT NULL,
    status TEXT NOT NULL
);
