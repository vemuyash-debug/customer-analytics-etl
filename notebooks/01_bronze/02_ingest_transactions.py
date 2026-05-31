# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze Layer: Ingest Transactions
# MAGIC Incremental ingestion of purchase transactions with year/month partitioning.

# COMMAND ----------

dbutils.widgets.text("process_date", "", "Process Date (YYYY-MM-DD, optional)")

# COMMAND ----------

import sys
from datetime import datetime

sys.path.insert(0, "/Workspace/Repos/customer-analytics-etl-databricks")

from src.config.settings import PipelineConfig
from src.bronze.ingestion import ingest_transactions_bronze
from src.utils.logging_utils import AuditLogger

# COMMAND ----------

config = PipelineConfig()
process_date_str = dbutils.widgets.get("process_date")
process_date = datetime.strptime(process_date_str, "%Y-%m-%d").date() if process_date_str else None

audit = AuditLogger(spark, config.table("audit", "pipeline_runs"))

# COMMAND ----------

record_count = ingest_transactions_bronze(
    spark,
    config.path("landing", "transactions"),
    config.table("bronze", "transactions"),
    config.path("bronze", "transactions"),
    process_date=process_date,
)

audit.log_event(
    "customer_analytics_etl",
    "bronze",
    "ingest_transactions",
    "SUCCESS",
    records_processed=record_count,
)

display(spark.table(config.table("bronze", "transactions")).limit(10))
