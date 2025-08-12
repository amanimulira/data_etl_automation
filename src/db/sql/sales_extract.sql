SELECT s.order_id, s.sale_date, s.amount, c.customer_name, c.region
FROM sales_transactions s
INNER JOIN customers c ON s.customer_id = c.customer_id
WHERE s.sale_date >= '2024-01-01' AND s.status = 'completed'
-- only pull completed sales since 2024 to reduce transfer size