# Athena Queries Used

'''
- View sample data:

SELECT *
FROM ecommerce_db.tbl_processed
LIMIT 10;



- Sales by Category

SELECT 
    category,
    COUNT(*) as transaction_count,
    SUM(total_amount) as total_sales,
    ROUND(AVG(total_amount), 2) as avg_order_value
FROM ecommerce_db.your_table_name
GROUP BY category
ORDER BY total_sales DESC;


- Hourly Sales Trend

SELECT 
    hour,
    COUNT(*) as transactions,
    SUM(total_amount) as revenue
FROM ecommerce_db.your_table_name
GROUP BY hour
ORDER BY hour;

- Top Products

SELECT 
    product_name,
    category,
    COUNT(*) as times_sold,
    SUM(quantity) as total_quantity,
    SUM(total_amount) as revenue
FROM ecommerce_db.your_table_name
GROUP BY product_name, category
ORDER BY revenue DESC
LIMIT 10;


- Customer Analysis

SELECT 
    customer_id,
    COUNT(DISTINCT transaction_id) as num_orders,
    SUM(total_amount) as lifetime_value,
    ROUND(AVG(total_amount), 2) as avg_order
FROM ecommerce_db.your_table_name
GROUP BY customer_id
ORDER BY lifetime_value DESC
LIMIT 20;
'''