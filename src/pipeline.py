"""Pipeline orchestration for end-to-end Medallion ETL execution."""

from __future__ import annotations

import time
from datetime import date

from pyspark.sql import SparkSession

from src.bronze.ingestion import (
    ingest_activity_logs_bronze,
    ingest_customers_bronze,
    ingest_transactions_bronze,
)
from src.config.settings import PipelineConfig
from src.data_quality.validators import DataQualityValidator
from src.gold.analytics import (
    build_activity_scores,
    build_customer_lifetime_value,
    build_customer_segmentation,
    build_monthly_growth,
    build_revenue_metrics,
    optimize_delta_table,
    vacuum_delta_table,
    write_gold_table,
)
from src.silver.transformations import (
    merge_silver_incremental,
    transform_activity_logs_silver,
    transform_customers_silver,
    transform_transactions_silver,
)
from src.utils.logging_utils import AuditLogger


class CustomerAnalyticsPipeline:
    """Orchestrates Bronze → Silver → Gold ETL with audit and data quality."""

    def __init__(self, spark: SparkSession, config: PipelineConfig | None = None) -> None:
        self.spark = spark
        self.config = config or PipelineConfig()
        self.audit = AuditLogger(
            spark,
            self.config.table("audit", "pipeline_runs"),
        )
        self.dq = DataQualityValidator(
            spark,
            self.config.table("audit", "data_quality"),
            null_threshold_pct=self.config.get("data_quality", "null_threshold_pct"),
            duplicate_threshold_pct=self.config.get("data_quality", "duplicate_threshold_pct"),
            min_record_count=self.config.get("data_quality", "min_record_count"),
        )

    def run_bronze(self, process_date: date | None = None) -> dict[str, int]:
        start = time.time()
        counts = {}

        counts["customers"] = ingest_customers_bronze(
            self.spark,
            self.config.path("landing", "customers"),
            self.config.table("bronze", "customers"),
            self.config.path("bronze", "customers"),
            process_date=process_date,
        )
        counts["transactions"] = ingest_transactions_bronze(
            self.spark,
            self.config.path("landing", "transactions"),
            self.config.table("bronze", "transactions"),
            self.config.path("bronze", "transactions"),
            process_date=process_date,
        )
        counts["activity_logs"] = ingest_activity_logs_bronze(
            self.spark,
            self.config.path("landing", "activity_logs"),
            self.config.table("bronze", "activity_logs"),
            self.config.path("bronze", "activity_logs"),
            process_date=process_date,
        )

        self.audit.log_event(
            "customer_analytics_etl",
            "bronze",
            "ingest_all",
            "SUCCESS",
            records_processed=sum(counts.values()),
            duration_seconds=time.time() - start,
            metadata=counts,
        )
        return counts

    def run_silver(self) -> dict[str, int]:
        start = time.time()
        counts = {}
        dq_config = self.config.get("data_quality", "required_columns")

        bronze_customers = self.spark.table(self.config.table("bronze", "customers"))
        silver_customers = transform_customers_silver(bronze_customers)
        self.dq.run_all_checks(
            silver_customers,
            self.config.table("silver", "customers"),
            "silver",
            dq_config["customers"],
            ["customer_id"],
            self.audit.run_id,
        )
        counts["customers"] = merge_silver_incremental(
            self.spark,
            silver_customers,
            self.config.table("silver", "customers"),
            ["customer_id"],
            ["registration_year"],
        )

        bronze_txn = self.spark.table(self.config.table("bronze", "transactions"))
        silver_txn = transform_transactions_silver(bronze_txn)
        self.dq.run_all_checks(
            silver_txn,
            self.config.table("silver", "transactions"),
            "silver",
            dq_config["transactions"],
            ["transaction_id"],
            self.audit.run_id,
        )
        counts["transactions"] = merge_silver_incremental(
            self.spark,
            silver_txn,
            self.config.table("silver", "transactions"),
            ["transaction_id"],
            ["transaction_year", "transaction_month"],
        )

        bronze_activity = self.spark.table(self.config.table("bronze", "activity_logs"))
        silver_activity = transform_activity_logs_silver(bronze_activity)
        self.dq.run_all_checks(
            silver_activity,
            self.config.table("silver", "activity_logs"),
            "silver",
            dq_config["activity_logs"],
            ["event_id"],
            self.audit.run_id,
        )
        counts["activity_logs"] = merge_silver_incremental(
            self.spark,
            silver_activity,
            self.config.table("silver", "activity_logs"),
            ["event_id"],
            ["event_date"],
        )

        self.audit.log_event(
            "customer_analytics_etl",
            "silver",
            "transform_all",
            "SUCCESS",
            records_processed=sum(counts.values()),
            duration_seconds=time.time() - start,
            metadata=counts,
        )
        return counts

    def run_gold(self) -> dict[str, int]:
        start = time.time()
        counts = {}

        customers = self.spark.table(self.config.table("silver", "customers"))
        transactions = self.spark.table(self.config.table("silver", "transactions"))
        activity = self.spark.table(self.config.table("silver", "activity_logs"))

        clv = build_customer_lifetime_value(customers, transactions)
        counts["customer_lifetime_value"] = write_gold_table(
            clv, self.config.table("gold", "customer_lifetime_value")
        )

        segmentation = build_customer_segmentation(
            clv,
            self.config.get("segmentation", "rfm", "recency_days"),
            self.config.get("segmentation", "rfm", "frequency_bins"),
            self.config.get("segmentation", "rfm", "monetary_bins"),
        )
        counts["customer_segmentation"] = write_gold_table(
            segmentation, self.config.table("gold", "customer_segmentation")
        )

        revenue = build_revenue_metrics(transactions, customers)
        counts["revenue_metrics"] = write_gold_table(
            revenue,
            self.config.table("gold", "revenue_metrics"),
            partition_columns=["year", "month"],
        )

        activity_scores = build_activity_scores(activity, customers)
        counts["activity_scores"] = write_gold_table(
            activity_scores, self.config.table("gold", "activity_scores")
        )

        growth = build_monthly_growth(customers)
        counts["monthly_growth"] = write_gold_table(
            growth,
            self.config.table("gold", "monthly_growth"),
            partition_columns=["year"],
        )

        self.audit.log_event(
            "customer_analytics_etl",
            "gold",
            "build_analytics",
            "SUCCESS",
            records_processed=sum(counts.values()),
            duration_seconds=time.time() - start,
            metadata=counts,
        )
        return counts

    def run_maintenance(self) -> None:
        z_order = self.config.get("processing", "z_order_columns")
        retention = self.config.get("processing", "vacuum_retention_hours")

        maintenance_targets = [
            ("bronze", "customers"),
            ("bronze", "transactions"),
            ("silver", "customers"),
            ("silver", "transactions"),
            ("gold", "customer_lifetime_value"),
            ("gold", "revenue_metrics"),
        ]
        for layer, table in maintenance_targets:
            full_name = self.config.table(layer, table)
            if self.spark.catalog.tableExists(full_name):
                optimize_delta_table(self.spark, full_name, z_order.get(table))
                vacuum_delta_table(self.spark, full_name, retention)

        self.audit.log_event(
            "customer_analytics_etl",
            "maintenance",
            "optimize_vacuum",
            "SUCCESS",
        )

    def run_full(self, process_date: date | None = None) -> dict[str, dict[str, int]]:
        return {
            "bronze": self.run_bronze(process_date),
            "silver": self.run_silver(),
            "gold": self.run_gold(),
        }
