-- name: products_by_category
SELECT
    p.sku,
    p.name,
    p.stock,
    s.name AS supplier_name
FROM products p
JOIN suppliers s ON s.id = p.supplier_id
WHERE p.category = :category
ORDER BY p.stock ASC, p.name ASC;
