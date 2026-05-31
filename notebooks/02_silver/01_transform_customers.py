# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Silver Layer: Transform Customers
# MAGIC Cleansing, deduplication, email standardization, and MERGE upsert into Silver.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/customer-analytics-etl-databricks")

from src.config.settings import PipelineConfig
from src.data_quality.validators import DataQualityValidator
from src.silver.transformations import merge_silver_incremental, transform_customers_silver
from src.utils.logging_utils import AuditLogger

# COMMAND ----------

config = PipelineConfig()
audit = AuditLogger(spark, config.table("audit", "pipeline_runs"))
dq = DataQualityValidator(spark, config.table("audit", "data_quality"))

bronze_df = spark.table(config.table("bronze", "customers"))
silver_df = transform_customers_silver(bronze_df)

# COMMAND ----------

dq.run_all_checks(
    silver_df,
    config.table("silver", "customers"),
    "silver",
    config.get("data_quality", "required_columns")["customers"],
    ["customer_id"],
    audit.run_id,
)

# COMMAND ----------

record_count = merge_silver_incremental(
    spark,
    silver_df,
    config.table("silver", "customers"),
    ["customer_id"],
    ["registration_year"],
)

audit.log_event(
    "customer_analytics_etl",
    "silver",
    "transform_customers",
    "SUCCESS",
    records_processed=record_count,
)

display(spark.table(config.table("silver", "customers")).limit(10))
