-- Sample Athena Queries for E-Commerce Data Pipeline

-- Query 1: Total Sales by Category
SELECT 
    category,
    COUNT(*) as transaction_count,
    SUM(total_amount) as total_sales,
    ROUND(AVG(total_amount), 2) as avg_order_value,
    MIN(total_amount) as min_order,
    MAX(total_amount) as max_order
FROM ecommerce_db.your_table_name
GROUP BY category
ORDER BY total_sales DESC;

-- Query 2: Hourly Sales Pattern
SELECT 
    hour,
    COUNT(*) as transactions,
    SUM(total_amount) as revenue,
    ROUND(AVG(total_amount), 2) as avg_order
FROM ecommerce_db.your_table_name
GROUP BY hour
ORDER BY hour;

-- Query 3: Top 20 Customers by Lifetime Value
SELECT 
    customer_id,
    COUNT(DISTINCT transaction_id) as num_orders,
    SUM(total_amount) as lifetime_value,
    ROUND(AVG(total_amount), 2) as avg_order,
    MIN(timestamp) as first_purchase,
    MAX(timestamp) as last_purchase
FROM ecommerce_db.your_table_name
GROUP BY customer_id
ORDER BY lifetime_value DESC
LIMIT 20;

-- Query 4: Product Performance
SELECT 
    product_name,
    category,
    COUNT(*) as times_sold,
    SUM(quantity) as total_quantity_sold,
    SUM(total_amount) as total_revenue,
    ROUND(AVG(price), 2) as avg_price
FROM ecommerce_db.your_table_name
GROUP BY product_name, category
ORDER BY total_revenue DESC;

-- Query 5: Daily Sales Trend
SELECT 
    DATE(timestamp) as sale_date,
    COUNT(*) as daily_transactions,
    SUM(total_amount) as daily_revenue,
    ROUND(AVG(total_amount), 2) as avg_transaction
FROM ecommerce_db.your_table_name
GROUP BY DATE(timestamp)
ORDER BY sale_date DESC;

-- Query 6: Day of Week Analysis
SELECT 
    CASE day_of_week
        WHEN 0 THEN 'Monday'
        WHEN 1 THEN 'Tuesday'
        WHEN 2 THEN 'Wednesday'
        WHEN 3 THEN 'Thursday'
        WHEN 4 THEN 'Friday'
        WHEN 5 THEN 'Saturday'
        WHEN 6 THEN 'Sunday'
    END as day_name,
    COUNT(*) as transactions,
    SUM(total_amount) as revenue
FROM ecommerce_db.your_table_name
GROUP BY day_of_week
ORDER BY day_of_week;

-- Query 7: High-Value Transactions
SELECT 
    transaction_id,
    timestamp,
    customer_id,
    product_name,
    quantity,
    total_amount
FROM ecommerce_db.your_table_name
WHERE total_amount > 1000
ORDER BY total_amount DESC;