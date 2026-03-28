SELECT
    p.sku,
    p.name,
    p.category,
    p.stock,
    p.reorder_point,
    s.name AS supplier_name,
    s.lead_time_days
FROM products p
JOIN suppliers s ON s.id = p.supplier_id
WHERE p.stock <= p.reorder_point
ORDER BY p.stock ASC, p.name ASC;
