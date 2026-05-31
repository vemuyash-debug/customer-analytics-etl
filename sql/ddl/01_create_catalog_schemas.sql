-- Unity Catalog setup for Customer Analytics ETL Pipeline
-- Run once in Databricks SQL or setup notebook

CREATE CATALOG IF NOT EXISTS customer_analytics;
CREATE SCHEMA IF NOT EXISTS customer_analytics.bronze COMMENT 'Raw ingested data - Bronze layer';
CREATE SCHEMA IF NOT EXISTS customer_analytics.silver COMMENT 'Cleaned and validated data - Silver layer';
CREATE SCHEMA IF NOT EXISTS customer_analytics.gold COMMENT 'Business analytics - Gold layer';
CREATE SCHEMA IF NOT EXISTS customer_analytics.audit COMMENT 'Pipeline audit and data quality logs';

-- External locations (update storage account and credentials)
-- CREATE EXTERNAL LOCATION IF NOT EXISTS customer_analytics_landing
--   URL 'abfss://customer-analytics@<storage_account>.dfs.core.windows.net/landing'
--   WITH (STORAGE CREDENTIAL `adls_credential`);

-- Grant permissions (adjust groups for your environment)
-- GRANT USE CATALOG ON CATALOG customer_analytics TO `data_engineers`;
-- GRANT ALL PRIVILEGES ON SCHEMA customer_analytics.bronze TO `data_engineers`;
-- GRANT ALL PRIVILEGES ON SCHEMA customer_analytics.silver TO `data_engineers`;
-- GRANT ALL PRIVILEGES ON SCHEMA customer_analytics.gold TO `data_engineers`;
-- GRANT SELECT ON SCHEMA customer_analytics.gold TO `data_analysts`;
