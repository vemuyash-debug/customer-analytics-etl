# Databricks notebook source
# MAGIC %md
# MAGIC # 13 - Data Quality Dashboard
# MAGIC Operational monitoring of pipeline runs and data quality check results.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/customer-analytics-etl-databricks")

from src.config.settings import PipelineConfig

config = PipelineConfig()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recent Pipeline Runs

# COMMAND ----------

display(
    spark.table(config.table("audit", "pipeline_runs"))
    .orderBy("event_timestamp", ascending=False)
    .limit(50)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Check Summary

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            table_name,
            check_name,
            status,
            COUNT(*) AS check_count,
            MAX(check_timestamp) AS last_check
        FROM {config.table("audit", "data_quality")}
        GROUP BY table_name, check_name, status
        ORDER BY last_check DESC
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Failed Quality Checks (Last 7 Days)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT *
        FROM {config.table("audit", "data_quality")}
        WHERE status = 'FAILED'
          AND check_timestamp >= current_timestamp() - INTERVAL 7 DAYS
        ORDER BY check_timestamp DESC
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Layer Record Counts

# COMMAND ----------

layers = [
    ("bronze", ["customers", "transactions", "activity_logs"]),
    ("silver", ["customers", "transactions", "activity_logs"]),
    ("gold", ["customer_lifetime_value", "customer_segmentation", "revenue_metrics", "activity_scores"]),
]

for layer, tables in layers:
    for table in tables:
        full_name = config.table(layer, table)
        if spark.catalog.tableExists(full_name):
            count = spark.table(full_name).count()
            print(f"{full_name}: {count:,} records")
