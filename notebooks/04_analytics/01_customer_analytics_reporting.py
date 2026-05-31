# Databricks notebook source
# MAGIC %md
# MAGIC # 11 - Analytics & Reporting
# MAGIC Spark SQL queries for business insights and Power BI consumption.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top customers by revenue
# MAGIC SELECT customer_id, first_name, last_name, lifetime_revenue, total_orders, average_order_value
# MAGIC FROM customer_analytics.gold.customer_lifetime_value
# MAGIC ORDER BY lifetime_revenue DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer segmentation summary
# MAGIC SELECT s.customer_segment, COUNT(*) AS customers, ROUND(AVG(c.lifetime_revenue), 2) AS avg_clv
# MAGIC FROM customer_analytics.gold.customer_segmentation s
# MAGIC JOIN customer_analytics.gold.customer_lifetime_value c USING (customer_id)
# MAGIC GROUP BY s.customer_segment
# MAGIC ORDER BY avg_clv DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monthly revenue trend
# MAGIC SELECT year, month, SUM(total_revenue) AS monthly_revenue, SUM(unique_customers) AS active_customers
# MAGIC FROM customer_analytics.gold.revenue_metrics
# MAGIC GROUP BY year, month
# MAGIC ORDER BY year, month

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Geographic revenue
# MAGIC SELECT country, state, SUM(total_revenue) AS revenue, SUM(transaction_count) AS orders
# MAGIC FROM customer_analytics.gold.revenue_metrics
# MAGIC GROUP BY country, state
# MAGIC ORDER BY revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Engagement vs revenue correlation
# MAGIC SELECT
# MAGIC   a.engagement_score,
# MAGIC   a.total_events,
# MAGIC   c.lifetime_revenue,
# MAGIC   c.customer_tier
# MAGIC FROM customer_analytics.gold.activity_scores a
# MAGIC JOIN customer_analytics.gold.customer_lifetime_value c USING (customer_id)
# MAGIC ORDER BY a.engagement_score DESC
