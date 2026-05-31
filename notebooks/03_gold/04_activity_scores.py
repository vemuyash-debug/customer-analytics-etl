# Databricks notebook source
# MAGIC %md
# MAGIC # 10 - Gold Layer: Activity Scores & Monthly Growth
# MAGIC Customer engagement scoring and registration growth trends.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/customer-analytics-etl-databricks")

from src.config.settings import PipelineConfig
from src.gold.analytics import build_activity_scores, build_monthly_growth, write_gold_table
from src.utils.logging_utils import AuditLogger

# COMMAND ----------

config = PipelineConfig()
audit = AuditLogger(spark, config.table("audit", "pipeline_runs"))

customers = spark.table(config.table("silver", "customers"))
activity = spark.table(config.table("silver", "activity_logs"))

activity_df = build_activity_scores(activity, customers)
activity_count = write_gold_table(activity_df, config.table("gold", "activity_scores"))

growth_df = build_monthly_growth(customers)
growth_count = write_gold_table(
    growth_df,
    config.table("gold", "monthly_growth"),
    partition_columns=["year"],
)

audit.log_event(
    "customer_analytics_etl",
    "gold",
    "activity_scores_and_growth",
    "SUCCESS",
    records_processed=activity_count + growth_count,
)

display(spark.table(config.table("gold", "activity_scores")).orderBy("engagement_score", ascending=False).limit(10))
