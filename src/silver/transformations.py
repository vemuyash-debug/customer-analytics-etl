"""Silver layer transformations - cleansing, deduplication, and business rules."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def standardize_email(df: DataFrame) -> DataFrame:
    return df.withColumn("email", F.lower(F.trim(F.col("email"))))


def standardize_names(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
    )


def deduplicate_by_key(
    df: DataFrame,
    key_columns: list[str],
    order_column: str = "_ingestion_timestamp",
) -> DataFrame:
    window = Window.partitionBy(*key_columns).orderBy(F.col(order_column).desc())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def transform_customers_silver(bronze_df: DataFrame) -> DataFrame:
    df = standardize_email(bronze_df)
    df = standardize_names(df)
    df = df.filter(F.col("customer_id").isNotNull() & F.col("email").isNotNull())
    df = df.filter(F.col("email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$"))
    df = deduplicate_by_key(df, ["customer_id"])
    return df.select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        F.to_date("registration_date").alias("registration_date"),
        "city",
        "state",
        "country",
        "customer_tier",
        F.col("registration_year").cast("int"),
        "_ingestion_timestamp",
        "_source_system",
        "_batch_id",
    )


def transform_transactions_silver(bronze_df: DataFrame) -> DataFrame:
    df = bronze_df.filter(
        F.col("transaction_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & (F.col("amount").cast("double") > 0)
    )
    df = deduplicate_by_key(df, ["transaction_id"])
    return df.select(
        "transaction_id",
        "customer_id",
        "order_id",
        F.col("amount").cast("double").alias("amount"),
        F.col("quantity").cast("int").alias("quantity"),
        "product_category",
        "payment_method",
        F.to_date("transaction_date").alias("transaction_date"),
        F.col("transaction_year").cast("int"),
        F.col("transaction_month").cast("int"),
        "_ingestion_timestamp",
        "_source_system",
    )


def transform_activity_logs_silver(bronze_df: DataFrame) -> DataFrame:
    df = bronze_df.filter(
        F.col("event_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("event_type").isNotNull()
    )
    df = deduplicate_by_key(df, ["event_id"])
    return df.select(
        "event_id",
        "customer_id",
        "event_type",
        F.to_timestamp("event_timestamp").alias("event_timestamp"),
        F.to_date("event_timestamp").alias("event_date"),
        "session_id",
        "page_url",
        "device_type",
        F.col("duration_seconds").cast("int").alias("duration_seconds"),
        "_ingestion_timestamp",
        "_source_system",
    )


def merge_silver_incremental(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    merge_keys: list[str],
    partition_columns: list[str] | None = None,
) -> int:
    from delta.tables import DeltaTable

    if not spark.catalog.tableExists(target_table):
        writer = source_df.write.format("delta").mode("overwrite")
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        writer.saveAsTable(target_table)
        return source_df.count()

    target = DeltaTable.forName(spark, target_table)
    condition = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)

    (
        target.alias("target")
        .merge(source_df.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return source_df.count()
