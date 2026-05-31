-- Gold layer analytics queries for reporting and Power BI

-- Top 10 customers by lifetime revenue
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    country,
    customer_tier,
    lifetime_revenue,
    total_orders,
    average_order_value,
    days_since_last_purchase
FROM customer_analytics.gold.customer_lifetime_value
ORDER BY lifetime_revenue DESC
LIMIT 10;

-- Customer segmentation distribution
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(lifetime_revenue), 2) AS avg_lifetime_revenue,
    ROUND(AVG(total_orders), 2) AS avg_orders
FROM customer_analytics.gold.customer_segmentation s
JOIN customer_analytics.gold.customer_lifetime_value c USING (customer_id)
GROUP BY customer_segment
ORDER BY customer_count DESC;

-- Monthly revenue trends
SELECT
    year,
    month,
    SUM(total_revenue) AS monthly_revenue,
    SUM(transaction_count) AS total_transactions,
    SUM(unique_customers) AS active_customers
FROM customer_analytics.gold.revenue_metrics
GROUP BY year, month
ORDER BY year, month;

-- Geographic revenue analysis
SELECT
    country,
    state,
    SUM(total_revenue) AS total_revenue,
    SUM(transaction_count) AS transaction_count,
    ROUND(AVG(avg_transaction_value), 2) AS avg_transaction_value
FROM customer_analytics.gold.revenue_metrics
GROUP BY country, state
ORDER BY total_revenue DESC;

-- Monthly customer growth
SELECT
    year,
    month,
    country,
    new_customers,
    SUM(new_customers) OVER (PARTITION BY country ORDER BY year, month) AS cumulative_customers
FROM customer_analytics.gold.monthly_growth
ORDER BY year, month, country;

-- Customer engagement leaderboard
SELECT
    a.customer_id,
    a.email,
    a.customer_tier,
    a.engagement_score,
    a.total_events,
    a.total_sessions,
    c.lifetime_revenue
FROM customer_analytics.gold.activity_scores a
LEFT JOIN customer_analytics.gold.customer_lifetime_value c USING (customer_id)
ORDER BY a.engagement_score DESC
LIMIT 20;

-- Purchase behavior by product category
SELECT
    product_category,
    SUM(transaction_count) AS orders,
    SUM(total_revenue) AS revenue,
    ROUND(SUM(total_revenue) / SUM(transaction_count), 2) AS avg_order_value
FROM customer_analytics.gold.revenue_metrics
GROUP BY product_category
ORDER BY revenue DESC;
