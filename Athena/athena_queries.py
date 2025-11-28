# Athena Queries Used

'''
- Total sales by category:

SELECT
    category,
    COUNT(*) as total_sales,
    AVG(total_amount) as avg_order_value
FROM ecommerce_db.transactions
GROUP BY category
ORDER BY total_sales DESC;



- Top customers

SELECT
    customer_id,
    COUNT(*) as order_count,
    SUM(total_amount) as lifetime_value
FROM ecommerce_db.transactions
GROUP BY customer_id
ORDER BY lifetime_value DESC
LIMIT 10
'''