# Architecture Documentation

## Overview

This platform simulates a retail company's customer analytics data engineering environment. Daily batches of customer registrations, purchase transactions, and web activity logs flow from source systems into ADLS Gen2, through a Medallion pipeline, and into business-ready Gold datasets.

## Data Sources

| Source | Format | System | Frequency |
|---|---|---|---|
| Customer registrations | CSV (structured) | CRM | Daily |
| Purchase transactions | CSV (structured) | POS | Daily |
| Web activity logs | JSON (semi-structured) | Web Analytics | Daily |

## Layer Details

### Bronze Layer

**Purpose**: Preserve raw source data with ingestion metadata.

**Patterns**:
- Append-only Delta writes with `mergeSchema` enabled
- Partitioning by date dimensions (`registration_year`, `transaction_year/month`, `event_date`)
- Ingestion metadata columns: `_ingestion_timestamp`, `_source_system`, `_ingestion_mode`, `_batch_id`

**Tables**: `bronze.customers`, `bronze.transactions`, `bronze.activity_logs`

### Silver Layer

**Purpose**: Clean, validate, deduplicate, and enforce business rules.

**Transformations**:
- Email lowercasing and trim
- Name standardization (initcap)
- Invalid email filtering (regex)
- Negative/zero amount filtering
- Deduplication via window functions (latest record wins)
- Delta MERGE for incremental upserts

**Data Quality Gates** (must pass before write):
- Record count validation
- Schema validation (required columns present)
- Null percentage check (configurable threshold)
- Duplicate key detection

**Tables**: `silver.customers`, `silver.transactions`, `silver.activity_logs`

### Gold Layer

**Purpose**: Business-ready analytics datasets for reporting and ML features.

| Dataset | Key Metrics |
|---|---|
| Customer Lifetime Value | total_orders, lifetime_revenue, AOV, days_since_last_purchase |
| Customer Segmentation | RFM scores, segment labels |
| Revenue Metrics | monthly/geo/category revenue, unique customers |
| Activity Scores | engagement_score, sessions, events |
| Monthly Growth | new customer counts by country |

## Delta Lake Operations

### OPTIMIZE + Z-ORDER
Applied post-pipeline to co-locate related data for query performance:
- Customers: Z-ORDER BY `(customer_id, email)`
- Transactions: Z-ORDER BY `(customer_id, transaction_id)`

### VACUUM
Removes files older than retention period (default 168 hours / 7 days).

### Time Travel
Historical table versions queryable via `VERSION AS OF` for audit and rollback scenarios.

## Performance Optimizations

1. **Partitioning**: Bronze/Silver/Gold tables partitioned by time dimensions
2. **Broadcast Joins**: Customer dimension broadcast in revenue metrics
3. **Adaptive Query Execution**: Enabled via Spark config
4. **Delta Auto-Compact / Optimize Write**: Databricks-native write optimization
5. **IO Cache**: Enabled for repeated reads in Gold layer builds

## Monitoring & Observability

### Audit Tables
- `audit.pipeline_runs`: Run ID, layer, step, status, duration, record counts
- `audit.data_quality_checks`: Check results with thresholds and timestamps

### Dashboard Notebook
`02_data_quality_dashboard.py` provides:
- Recent pipeline run history
- DQ check pass/fail summary
- Layer record counts

## Security & Governance

- Unity Catalog for centralized metadata and access control
- Schema-level permissions (engineers vs analysts)
- External locations for ADLS Gen2 paths
- No secrets in code — storage account via environment variables

## Deployment

### Databricks Workflow
Daily scheduled job (`customer_analytics_etl_daily`) with task dependencies:
Bronze (parallel) → Silver (parallel) → Gold (sequential) → Maintenance → Dashboard

### Environment Configuration
| Variable | Purpose |
|---|---|
| `STORAGE_ACCOUNT_NAME` | ADLS Gen2 account |
| Widget: `catalog_name` | Unity Catalog name |
| Widget: `process_date` | Optional incremental date filter |

## Future Enhancements

- Structured Streaming for near-real-time activity logs
- Great Expectations or Databricks DQ expectations integration
- Power BI semantic model connected to Gold tables
- Feature Store for ML customer churn models
- Auto Loader for file arrival detection
