"""Bronze layer ingestion - raw data preservation with incremental loading."""

from __future__ import annotations

from datetime import date
from typing import Literal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from src.utils.logging_utils import add_ingestion_metadata


SourceFormat = Literal["csv", "json", "parquet"]


def read_landing_data(
    spark: SparkSession,
    landing_path: str,
    source_format: SourceFormat,
    schema: StructType | None = None,
    process_date: date | None = None,
) -> DataFrame:
    reader = spark.read.format(source_format).option("header", "true").option("inferSchema", "false")
    if source_format == "json":
        reader = reader.option("multiline", "false")
    if schema:
        reader = reader.schema(schema)

    path = landing_path
    if process_date:
        path = (
            f"{landing_path.rstrip('/')}/year={process_date.year}/"
            f"month={process_date.month:02d}/day={process_date.day:02d}"
        )

    return reader.load(path)


def write_bronze_delta(
    df: DataFrame,
    table_name: str,
    storage_path: str,
    partition_columns: list[str] | None = None,
    mode: str = "append",
) -> int:
    spark = df.sparkSession
    writer = (
        df.write.format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
    )
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.save(storage_path)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} "
        f"USING DELTA LOCATION '{storage_path}'"
    )
    return df.count()


def merge_bronze_incremental(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    merge_keys: list[str],
) -> int:
    """Incremental upsert into Bronze using Delta MERGE (CDC pattern)."""
    from delta.tables import DeltaTable

    if not spark.catalog.tableExists(target_table):
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
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


def ingest_customers_bronze(
    spark: SparkSession,
    landing_path: str,
    table_name: str,
    storage_path: str,
    source_system: str = "CRM",
    process_date: date | None = None,
) -> int:
    df = read_landing_data(spark, landing_path, "csv", process_date=process_date)
    df = add_ingestion_metadata(df, source_system, "incremental")
    df = df.withColumn("registration_year", F.year(F.to_date("registration_date")))
    return write_bronze_delta(
        df, table_name, storage_path, partition_columns=["registration_year"]
    )


def ingest_transactions_bronze(
    spark: SparkSession,
    landing_path: str,
    table_name: str,
    storage_path: str,
    source_system: str = "POS",
    process_date: date | None = None,
) -> int:
    df = read_landing_data(spark, landing_path, "csv", process_date=process_date)
    df = add_ingestion_metadata(df, source_system, "incremental")
    df = (
        df.withColumn("transaction_year", F.year(F.to_date("transaction_date")))
        .withColumn("transaction_month", F.month(F.to_date("transaction_date")))
    )
    return write_bronze_delta(
        df,
        table_name,
        storage_path,
        partition_columns=["transaction_year", "transaction_month"],
    )


def ingest_activity_logs_bronze(
    spark: SparkSession,
    landing_path: str,
    table_name: str,
    storage_path: str,
    source_system: str = "WebAnalytics",
    process_date: date | None = None,
) -> int:
    df = read_landing_data(spark, landing_path, "json", process_date=process_date)
    df = add_ingestion_metadata(df, source_system, "incremental")
    df = df.withColumn("event_date", F.to_date("event_timestamp"))
    return write_bronze_delta(
        df, table_name, storage_path, partition_columns=["event_date"]
    )
