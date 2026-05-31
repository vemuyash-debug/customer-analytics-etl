#!/usr/bin/env python3
"""Run the Customer Analytics ETL pipeline locally with sample data."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

LOCAL_ROOT = PROJECT_ROOT / "data" / "generated"
SAMPLE_ROOT = PROJECT_ROOT / "data" / "sample"


def setup_local_config():
    from src.config.settings import PipelineConfig

    config = PipelineConfig()
    base = str(LOCAL_ROOT.resolve())

    for layer in ("landing", "bronze", "silver", "gold", "audit"):
        for key in config.get("paths", layer, default={}):
            config.raw["paths"][layer][key] = f"{base}/{layer}/{key}"

    for layer in ("bronze", "silver", "gold", "audit"):
        for key in config.get("tables", layer, default={}):
            config.raw["tables"][layer][key] = f"{layer}.{key}"

    config.raw["storage"]["base_path"] = base
    return config


def create_spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = str((LOCAL_ROOT / "warehouse").resolve())
    builder = (
        SparkSession.builder.master("local[*]")
        .appName("CustomerAnalyticsETL-Local")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def stage_sample_data() -> None:
    if LOCAL_ROOT.exists():
        shutil.rmtree(LOCAL_ROOT)
    LOCAL_ROOT.mkdir(parents=True)

    mappings = [
        (SAMPLE_ROOT / "customers", LOCAL_ROOT / "landing" / "customers"),
        (SAMPLE_ROOT / "transactions", LOCAL_ROOT / "landing" / "transactions"),
        (SAMPLE_ROOT / "activity_logs", LOCAL_ROOT / "landing" / "activity_logs"),
    ]
    for src, dst in mappings:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(src, dst)


def create_schemas(spark) -> None:
    for schema in ("bronze", "silver", "gold", "audit"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")


def print_results(spark, config) -> None:
    print("\n" + "=" * 60)
    print("PIPELINE RESULTS")
    print("=" * 60)

    gold_tables = [
        "customer_lifetime_value",
        "customer_segmentation",
        "revenue_metrics",
        "activity_scores",
        "monthly_growth",
    ]
    for table in gold_tables:
        full = config.table("gold", table)
        if spark.catalog.tableExists(full):
            count = spark.table(full).count()
            print(f"  gold.{table}: {count} rows")

    print("\n--- Top 5 Customers by Lifetime Revenue ---")
    clv = config.table("gold", "customer_lifetime_value")
    spark.table(clv).select(
        "customer_id", "first_name", "last_name", "lifetime_revenue", "total_orders"
    ).orderBy("lifetime_revenue", ascending=False).show(5, truncate=False)

    print("--- Customer Segmentation Summary ---")
    seg = config.table("gold", "customer_segmentation")
    spark.table(seg).groupBy("customer_segment").count().orderBy("count", ascending=False).show()

    print("--- Monthly Revenue ---")
    rev = config.table("gold", "revenue_metrics")
    spark.table(rev).groupBy("year", "month").agg(
        {"total_revenue": "sum", "transaction_count": "sum"}
    ).orderBy("year", "month").show(10)


def main() -> int:
    print("Staging sample data...")
    stage_sample_data()

    print("Starting Spark session...")
    spark = create_spark()
    create_schemas(spark)

    config = setup_local_config()

    from src.pipeline import CustomerAnalyticsPipeline

    pipeline = CustomerAnalyticsPipeline(spark, config)

    print("\nRunning Bronze layer...")
    bronze = pipeline.run_bronze()
    print(f"  Bronze counts: {bronze}")

    print("\nRunning Silver layer...")
    silver = pipeline.run_silver()
    print(f"  Silver counts: {silver}")

    print("\nRunning Gold layer...")
    gold = pipeline.run_gold()
    print(f"  Gold counts: {gold}")

    print("\nRunning Delta maintenance...")
    pipeline.run_maintenance()

    print_results(spark, config)
    spark.stop()
    print("\nPipeline completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
