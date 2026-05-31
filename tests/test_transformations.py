"""Spark integration tests - run with local Spark or in Databricks."""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName("customer_analytics_tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def test_transform_customers_silver(spark: SparkSession) -> None:
    from src.silver.transformations import transform_customers_silver

    data = [
        ("C001", "john", "SMITH", " JOHN.SMITH@EMAIL.COM ", "555-0101", "2023-01-15", "NYC", "NY", "USA", "Gold", 2023),
        ("C001", "john", "SMITH", "john.smith@email.com", "555-0101", "2023-01-15", "NYC", "NY", "USA", "Gold", 2023),
    ]
    columns = [
        "customer_id", "first_name", "last_name", "email", "phone",
        "registration_date", "city", "state", "country", "customer_tier",
        "registration_year", "_ingestion_timestamp", "_source_system", "_batch_id",
    ]
    from pyspark.sql import functions as F

    bronze_df = spark.createDataFrame(
        [(d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9], d[10]) for d in data],
        ["customer_id", "first_name", "last_name", "email", "phone",
         "registration_date", "city", "state", "country", "customer_tier", "registration_year"],
    )
    bronze_df = (
        bronze_df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_system", F.lit("CRM"))
        .withColumn("_batch_id", F.lit("test-batch"))
    )

    silver_df = transform_customers_silver(bronze_df)
    assert silver_df.count() == 1
    row = silver_df.first()
    assert row.email == "john.smith@email.com"
    assert row.first_name == "John"


def test_build_customer_segmentation(spark: SparkSession) -> None:
    from src.gold.analytics import build_customer_segmentation

    clv_data = [
        ("C001", 5, 1500.0, 300.0, 10),
        ("C002", 1, 50.0, 50.0, 200),
    ]
    clv_df = spark.createDataFrame(
        clv_data,
        ["customer_id", "total_orders", "lifetime_revenue", "average_order_value", "days_since_last_purchase"],
    )

    seg_df = build_customer_segmentation(clv_df)
    segments = {row.customer_id: row.customer_segment for row in seg_df.collect()}
    assert segments["C001"] == "Champions"
    assert segments["C002"] == "Hibernating"
