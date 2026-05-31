# Databricks notebook source
# MAGIC %md
# MAGIC # 12 - Delta Lake Maintenance
# MAGIC OPTIMIZE, Z-ORDER, VACUUM, and Delta Time Travel demonstration.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/customer-analytics-etl-databricks")

from src.config.settings import PipelineConfig
from src.gold.analytics import optimize_delta_table, vacuum_delta_table
from src.utils.logging_utils import AuditLogger

# COMMAND ----------

config = PipelineConfig()
audit = AuditLogger(spark, config.table("audit", "pipeline_runs"))
z_order = config.get("processing", "z_order_columns")
retention = config.get("processing", "vacuum_retention_hours")

tables_to_optimize = [
    ("bronze", "customers"),
    ("bronze", "transactions"),
    ("silver", "customers"),
    ("silver", "transactions"),
    ("gold", "customer_lifetime_value"),
    ("gold", "revenue_metrics"),
]

# COMMAND ----------

for layer, table in tables_to_optimize:
    full_name = config.table(layer, table)
    if spark.catalog.tableExists(full_name):
        z_cols = z_order.get(table)
        print(f"Optimizing {full_name} with Z-ORDER on {z_cols}")
        optimize_delta_table(spark, full_name, z_cols)
        vacuum_delta_table(spark, full_name, retention)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Time Travel Demo
# MAGIC Query historical versions of the customer lifetime value table.

# COMMAND ----------

clv_table = config.table("gold", "customer_lifetime_value")

history_df = spark.sql(f"DESCRIBE HISTORY {clv_table}")
display(history_df)

# COMMAND ----------

if history_df.count() > 0:
    version = history_df.select("version").first()[0]
    historical_df = spark.sql(f"SELECT * FROM {clv_table} VERSION AS OF {version}")
    display(historical_df.limit(5))

audit.log_event(
    "customer_analytics_etl",
    "maintenance",
    "optimize_vacuum_time_travel",
    "SUCCESS",
)
