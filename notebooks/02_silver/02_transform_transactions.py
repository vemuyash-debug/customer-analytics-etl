# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Silver Layer: Transform Transactions
# MAGIC Business rule enforcement, amount validation, and incremental MERGE.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/customer-analytics-etl-databricks")

from src.config.settings import PipelineConfig
from src.data_quality.validators import DataQualityValidator
from src.silver.transformations import merge_silver_incremental, transform_transactions_silver
from src.utils.logging_utils import AuditLogger

# COMMAND ----------

config = PipelineConfig()
audit = AuditLogger(spark, config.table("audit", "pipeline_runs"))
dq = DataQualityValidator(spark, config.table("audit", "data_quality"))

bronze_df = spark.table(config.table("bronze", "transactions"))
silver_df = transform_transactions_silver(bronze_df)

dq.run_all_checks(
    silver_df,
    config.table("silver", "transactions"),
    "silver",
    config.get("data_quality", "required_columns")["transactions"],
    ["transaction_id"],
    audit.run_id,
)

record_count = merge_silver_incremental(
    spark,
    silver_df,
    config.table("silver", "transactions"),
    ["transaction_id"],
    ["transaction_year", "transaction_month"],
)

audit.log_event(
    "customer_analytics_etl",
    "silver",
    "transform_transactions",
    "SUCCESS",
    records_processed=record_count,
)

display(spark.table(config.table("silver", "transactions")).limit(10))
