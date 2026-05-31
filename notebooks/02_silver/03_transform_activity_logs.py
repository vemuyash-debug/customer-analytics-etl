# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Silver Layer: Transform Activity Logs
# MAGIC Schema validation and deduplication for semi-structured event data.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/customer-analytics-etl-databricks")

from src.config.settings import PipelineConfig
from src.data_quality.validators import DataQualityValidator
from src.silver.transformations import merge_silver_incremental, transform_activity_logs_silver
from src.utils.logging_utils import AuditLogger

# COMMAND ----------

config = PipelineConfig()
audit = AuditLogger(spark, config.table("audit", "pipeline_runs"))
dq = DataQualityValidator(spark, config.table("audit", "data_quality"))

bronze_df = spark.table(config.table("bronze", "activity_logs"))
silver_df = transform_activity_logs_silver(bronze_df)

dq.run_all_checks(
    silver_df,
    config.table("silver", "activity_logs"),
    "silver",
    config.get("data_quality", "required_columns")["activity_logs"],
    ["event_id"],
    audit.run_id,
)

record_count = merge_silver_incremental(
    spark,
    silver_df,
    config.table("silver", "activity_logs"),
    ["event_id"],
    ["event_date"],
)

audit.log_event(
    "customer_analytics_etl",
    "silver",
    "transform_activity_logs",
    "SUCCESS",
    records_processed=record_count,
)

display(spark.table(config.table("silver", "activity_logs")).limit(10))
