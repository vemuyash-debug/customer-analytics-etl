# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Initialize Environment
# MAGIC Sets up Unity Catalog, schemas, audit tables, and uploads sample landing data.

# COMMAND ----------

dbutils.widgets.text("storage_account", "devdatalake", "ADLS Storage Account")
dbutils.widgets.text("catalog_name", "customer_analytics", "Unity Catalog Name")

storage_account = dbutils.widgets.get("storage_account")
catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/customer-analytics-etl-databricks")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE CATALOG IF NOT EXISTS ${catalog_name};
# MAGIC -- CREATE SCHEMA IF NOT EXISTS ${catalog_name}.bronze;
# MAGIC -- CREATE SCHEMA IF NOT EXISTS ${catalog_name}.silver;
# MAGIC -- CREATE SCHEMA IF NOT EXISTS ${catalog_name}.gold;
# MAGIC -- CREATE SCHEMA IF NOT EXISTS ${catalog_name}.audit;

# COMMAND ----------

base_path = f"abfss://customer-analytics@{storage_account}.dfs.core.windows.net/customer-analytics"

landing_paths = {
    "customers": f"{base_path}/landing/customers",
    "transactions": f"{base_path}/landing/transactions",
    "activity_logs": f"{base_path}/landing/activity_logs",
}

for name, path in landing_paths.items():
    dbutils.fs.mkdirs(path)
    print(f"Created landing path: {path}")

# COMMAND ----------

# Upload sample data to ADLS landing zone
sample_base = "/Workspace/Repos/customer-analytics-etl-databricks/data/sample"

dbutils.fs.cp(
    f"file:{sample_base}/customers/customers_batch_001.csv",
    f"{landing_paths['customers']}/customers_batch_001.csv",
    True,
)
dbutils.fs.cp(
    f"file:{sample_base}/transactions/transactions_batch_001.csv",
    f"{landing_paths['transactions']}/transactions_batch_001.csv",
    True,
)
dbutils.fs.cp(
    f"file:{sample_base}/activity_logs/activity_batch_001.json",
    f"{landing_paths['activity_logs']}/activity_batch_001.json",
    True,
)

print("Sample data uploaded to landing zone.")

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

print("Spark configuration applied.")
