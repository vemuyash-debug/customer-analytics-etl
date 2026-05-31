# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - Gold Layer: Customer Segmentation (RFM)
# MAGIC RFM-based customer segmentation for targeted marketing.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/customer-analytics-etl-databricks")

from src.config.settings import PipelineConfig
from src.gold.analytics import build_customer_segmentation, write_gold_table
from src.utils.logging_utils import AuditLogger

# COMMAND ----------

config = PipelineConfig()
audit = AuditLogger(spark, config.table("audit", "pipeline_runs"))

clv_df = spark.table(config.table("gold", "customer_lifetime_value"))
rfm = config.get("segmentation", "rfm")

segmentation_df = build_customer_segmentation(
    clv_df,
    rfm["recency_days"],
    rfm["frequency_bins"],
    rfm["monetary_bins"],
)

record_count = write_gold_table(segmentation_df, config.table("gold", "customer_segmentation"))

audit.log_event(
    "customer_analytics_etl",
    "gold",
    "customer_segmentation",
    "SUCCESS",
    records_processed=record_count,
)

display(spark.table(config.table("gold", "customer_segmentation")).groupBy("customer_segment").count())
