# Customer Analytics ETL Pipeline

Enterprise-grade Customer Analytics Platform built on **Azure Databricks**, **PySpark**, **Delta Lake**, and **Azure Data Lake Storage Gen2**. Implements Medallion Architecture (Bronze → Silver → Gold) for a retail customer analytics use case.

## Architecture

```
Azure Data Lake Storage Gen2 (Landing Zone)
        │
        ▼
┌───────────────────────────────────────┐
│  Bronze Layer — Raw Delta Tables      │
│  customers | transactions | activity  │
└───────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────┐
│  Silver Layer — Cleaned & Validated   │
│  dedup | standardize | MERGE upsert   │
└───────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────┐
│  Gold Layer — Business Analytics      │
│  CLV | RFM Segmentation | Revenue     │
└───────────────────────────────────────┘
        │
        ▼
   Spark SQL Reporting / Power BI
```

## Features

| Capability | Implementation |
|---|---|
| Medallion Architecture | Bronze, Silver, Gold Delta tables |
| Incremental Loading | Delta MERGE upsert (CDC pattern) |
| Data Quality | Null, duplicate, schema, record count checks |
| Audit Logging | Pipeline run tracking in Delta audit tables |
| Delta Optimization | OPTIMIZE, Z-ORDER, VACUUM, Time Travel |
| Performance | Broadcast joins, partitioning, AQE, caching |
| Orchestration | Databricks Workflow job definition |
| Governance | Unity Catalog schemas and table naming |

## Project Structure

```
customer-analytics-etl-databricks/
├── config/                  # Pipeline and Spark configuration
├── src/
│   ├── bronze/              # Raw ingestion logic
│   ├── silver/              # Cleansing and validation
│   ├── gold/                # Business metrics
│   ├── data_quality/        # DQ validation framework
│   ├── utils/               # Spark session, logging, paths
│   └── pipeline.py          # End-to-end orchestrator
├── notebooks/               # Databricks notebooks (by layer)
├── data/sample/             # Sample CSV and JSON source files
├── sql/                     # DDL and analytics queries
├── databricks/
│   ├── workflows/           # Job definition JSON
│   └── init_scripts/        # Cluster init scripts
├── tests/                   # Unit and integration tests
└── docs/                    # Architecture documentation
```

## Gold Layer Outputs

| Table | Description |
|---|---|
| `customer_lifetime_value` | CLV, AOV, purchase frequency, recency |
| `customer_segmentation` | RFM-based segments (Champions, At Risk, etc.) |
| `revenue_metrics` | Monthly/geo/category revenue aggregations |
| `activity_scores` | Web engagement and activity scoring |
| `monthly_growth` | New customer registration trends |

## Quick Start

### 1. Prerequisites

- Azure Databricks workspace (Runtime 14.3+ with Delta Lake)
- ADLS Gen2 storage account with `customer-analytics` container
- Unity Catalog enabled
- Git repo connected to Databricks Repos

### 2. Clone and Configure

```bash
git clone <your-repo-url> customer-analytics-etl-databricks
cd customer-analytics-etl-databricks
```

Update `config/config.yaml` with your storage account name, or set:

```bash
export STORAGE_ACCOUNT_NAME=your_storage_account
```

### 3. Deploy to Databricks

1. Connect this repo via **Databricks Repos**
2. Run `notebooks/00_setup/00_initialize_environment.py` to create schemas and upload sample data
3. Run notebooks sequentially, or use `01_run_full_pipeline.py` for end-to-end execution
4. Import `databricks/workflows/customer_analytics_pipeline.json` as a Databricks Job

### 4. Run Locally (Tests)

```bash
pip install -r requirements.txt
pytest tests/ -v
```

## Notebook Execution Order

1. `00_initialize_environment` — Setup catalog, landing data
2. `01_ingest_customers` → `02_ingest_transactions` → `03_ingest_activity_logs`
3. `01_transform_customers` → `02_transform_transactions` → `03_transform_activity_logs`
4. `01_customer_lifetime_value` → `02_customer_segmentation` → `03_revenue_metrics` → `04_activity_scores`
5. `01_customer_analytics_reporting` — Business insights
6. `01_delta_optimize_vacuum` → `02_data_quality_dashboard`

## Business Insights (Sample Queries)

See `sql/analytics/customer_insights.sql` for:

- Top customers by revenue
- Customer segmentation distribution
- Monthly revenue trends
- Geographic revenue analysis
- Customer engagement leaderboard
- Purchase behavior by category


## Technology Stack

Azure Databricks · PySpark · Delta Lake · Spark SQL · ADLS Gen2 · Unity Catalog · Python · Databricks Workflows

## License

MIT
