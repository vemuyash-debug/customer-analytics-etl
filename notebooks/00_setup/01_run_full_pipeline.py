# Databricks notebook source
# MAGIC %md
# MAGIC # 14 - Full Pipeline Orchestration
# MAGIC End-to-end Bronze → Silver → Gold execution using the pipeline orchestrator.

# COMMAND ----------

dbutils.widgets.text("process_date", "", "Process Date (YYYY-MM-DD, optional)")

# COMMAND ----------

import sys
from datetime import datetime

sys.path.insert(0, "/Workspace/Repos/customer-analytics-etl-databricks")

from src.config.settings import PipelineConfig
from src.pipeline import CustomerAnalyticsPipeline

# COMMAND ----------

config = PipelineConfig()
pipeline = CustomerAnalyticsPipeline(spark, config)

process_date_str = dbutils.widgets.get("process_date")
process_date = datetime.strptime(process_date_str, "%Y-%m-%d").date() if process_date_str else None

results = pipeline.run_full(process_date)
print(results)

# COMMAND ----------

pipeline.run_maintenance()
print("Pipeline complete with maintenance tasks.")
