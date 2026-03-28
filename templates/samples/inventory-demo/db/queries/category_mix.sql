SELECT
    category,
    count(*) AS sku_count,
    sum(stock) AS units_on_hand
FROM products
GROUP BY category
ORDER BY units_on_hand DESC;
