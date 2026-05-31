# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Gold Layer: Customer Lifetime Value
# MAGIC Computes CLV, AOV, purchase frequency, and recency metrics.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/customer-analytics-etl-databricks")

from src.config.settings import PipelineConfig
from src.gold.analytics import build_customer_lifetime_value, write_gold_table
from src.utils.logging_utils import AuditLogger

# COMMAND ----------

config = PipelineConfig()
audit = AuditLogger(spark, config.table("audit", "pipeline_runs"))

customers = spark.table(config.table("silver", "customers"))
transactions = spark.table(config.table("silver", "transactions"))

clv_df = build_customer_lifetime_value(customers, transactions)
record_count = write_gold_table(clv_df, config.table("gold", "customer_lifetime_value"))

audit.log_event(
    "customer_analytics_etl",
    "gold",
    "customer_lifetime_value",
    "SUCCESS",
    records_processed=record_count,
)

display(spark.table(config.table("gold", "customer_lifetime_value")).orderBy("lifetime_revenue", ascending=False).limit(10))
