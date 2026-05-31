# Databricks notebook source
# MAGIC %md
# MAGIC # 09 - Gold Layer: Revenue Metrics
# MAGIC Geographic and category revenue analysis with broadcast join optimization.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/customer-analytics-etl-databricks")

from src.config.settings import PipelineConfig
from src.gold.analytics import build_revenue_metrics, write_gold_table
from src.utils.logging_utils import AuditLogger

# COMMAND ----------

config = PipelineConfig()
audit = AuditLogger(spark, config.table("audit", "pipeline_runs"))

customers = spark.table(config.table("silver", "customers"))
transactions = spark.table(config.table("silver", "transactions"))

revenue_df = build_revenue_metrics(transactions, customers)
record_count = write_gold_table(
    revenue_df,
    config.table("gold", "revenue_metrics"),
    partition_columns=["year", "month"],
)

audit.log_event(
    "customer_analytics_etl",
    "gold",
    "revenue_metrics",
    "SUCCESS",
    records_processed=record_count,
)

display(spark.table(config.table("gold", "revenue_metrics")).orderBy("total_revenue", ascending=False).limit(10))
