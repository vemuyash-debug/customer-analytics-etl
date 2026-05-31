"""Gold layer analytics - business metrics and customer insights."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_customer_lifetime_value(
    customers_df: DataFrame,
    transactions_df: DataFrame,
) -> DataFrame:
    txn_agg = transactions_df.groupBy("customer_id").agg(
        F.count("transaction_id").alias("total_orders"),
        F.sum("amount").alias("lifetime_revenue"),
        F.avg("amount").alias("average_order_value"),
        F.min("transaction_date").alias("first_purchase_date"),
        F.max("transaction_date").alias("last_purchase_date"),
        F.sum("quantity").alias("total_items_purchased"),
    )

    return (
        customers_df.alias("c")
        .join(txn_agg.alias("t"), "customer_id", "left")
        .select(
            F.col("c.customer_id"),
            F.col("c.first_name"),
            F.col("c.last_name"),
            F.col("c.email"),
            F.col("c.city"),
            F.col("c.state"),
            F.col("c.country"),
            F.col("c.customer_tier"),
            F.col("c.registration_date"),
            F.coalesce(F.col("t.total_orders"), F.lit(0)).alias("total_orders"),
            F.coalesce(F.col("t.lifetime_revenue"), F.lit(0.0)).alias("lifetime_revenue"),
            F.coalesce(F.col("t.average_order_value"), F.lit(0.0)).alias("average_order_value"),
            F.col("t.first_purchase_date"),
            F.col("t.last_purchase_date"),
            F.coalesce(F.col("t.total_items_purchased"), F.lit(0)).alias("total_items_purchased"),
            F.datediff(F.current_date(), F.col("t.last_purchase_date")).alias("days_since_last_purchase"),
            F.current_timestamp().alias("_updated_at"),
        )
    )


def build_customer_segmentation(
    clv_df: DataFrame,
    recency_bins: list[int] | None = None,
    frequency_bins: list[int] | None = None,
    monetary_bins: list[float] | None = None,
) -> DataFrame:
    recency_bins = recency_bins or [30, 90, 180]
    frequency_bins = frequency_bins or [1, 3, 6]
    monetary_bins = monetary_bins or [100, 500, 1000]

    df = clv_df.withColumn(
        "recency_score",
        F.when(F.col("days_since_last_purchase").isNull(), 1)
        .when(F.col("days_since_last_purchase") <= recency_bins[0], 4)
        .when(F.col("days_since_last_purchase") <= recency_bins[1], 3)
        .when(F.col("days_since_last_purchase") <= recency_bins[2], 2)
        .otherwise(1),
    ).withColumn(
        "frequency_score",
        F.when(F.col("total_orders") >= frequency_bins[2], 4)
        .when(F.col("total_orders") >= frequency_bins[1], 3)
        .when(F.col("total_orders") >= frequency_bins[0], 2)
        .otherwise(1),
    ).withColumn(
        "monetary_score",
        F.when(F.col("lifetime_revenue") >= monetary_bins[2], 4)
        .when(F.col("lifetime_revenue") >= monetary_bins[1], 3)
        .when(F.col("lifetime_revenue") >= monetary_bins[0], 2)
        .otherwise(1),
    )

    df = df.withColumn(
        "rfm_score",
        F.col("recency_score") + F.col("frequency_score") + F.col("monetary_score"),
    ).withColumn(
        "customer_segment",
        F.when(F.col("rfm_score") >= 10, "Champions")
        .when(F.col("rfm_score") >= 8, "Loyal Customers")
        .when(F.col("rfm_score") >= 6, "Potential Loyalists")
        .when(F.col("rfm_score") >= 4, "At Risk")
        .otherwise("Hibernating"),
    )

    return df.select(
        "customer_id",
        "recency_score",
        "frequency_score",
        "monetary_score",
        "rfm_score",
        "customer_segment",
        "lifetime_revenue",
        "total_orders",
        F.current_timestamp().alias("_updated_at"),
    )


def build_revenue_metrics(transactions_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    enriched = transactions_df.alias("t").join(
        F.broadcast(customers_df.select("customer_id", "state", "country", "customer_tier")),
        "customer_id",
    )

    monthly = enriched.groupBy(
        F.year("transaction_date").alias("year"),
        F.month("transaction_date").alias("month"),
        "country",
        "state",
        "product_category",
        "customer_tier",
    ).agg(
        F.count("transaction_id").alias("transaction_count"),
        F.sum("amount").alias("total_revenue"),
        F.avg("amount").alias("avg_transaction_value"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )

    return monthly.withColumn("_updated_at", F.current_timestamp())


def build_activity_scores(
    activity_df: DataFrame,
    customers_df: DataFrame,
) -> DataFrame:
    activity_agg = activity_df.groupBy("customer_id").agg(
        F.count("event_id").alias("total_events"),
        F.countDistinct("session_id").alias("total_sessions"),
        F.sum("duration_seconds").alias("total_engagement_seconds"),
        F.max("event_timestamp").alias("last_activity_timestamp"),
        F.countDistinct("event_type").alias("distinct_event_types"),
    )

    scored = activity_agg.withColumn(
        "engagement_score",
        F.least(
            F.lit(100),
            F.col("total_events") * 2
            + F.col("total_sessions") * 5
            + F.col("distinct_event_types") * 10,
        ),
    )

    return (
        customers_df.select("customer_id", "email", "customer_tier")
        .join(scored, "customer_id", "left")
        .select(
            "customer_id",
            "email",
            "customer_tier",
            F.coalesce(F.col("total_events"), F.lit(0)).alias("total_events"),
            F.coalesce(F.col("total_sessions"), F.lit(0)).alias("total_sessions"),
            F.coalesce(F.col("total_engagement_seconds"), F.lit(0)).alias("total_engagement_seconds"),
            F.col("last_activity_timestamp"),
            F.coalesce(F.col("engagement_score"), F.lit(0)).alias("engagement_score"),
            F.current_timestamp().alias("_updated_at"),
        )
    )


def build_monthly_growth(customers_df: DataFrame) -> DataFrame:
    return (
        customers_df.groupBy(
            F.year("registration_date").alias("year"),
            F.month("registration_date").alias("month"),
            "country",
        )
        .agg(F.count("customer_id").alias("new_customers"))
        .withColumn("_updated_at", F.current_timestamp())
    )


def write_gold_table(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
    partition_columns: list[str] | None = None,
) -> int:
    writer = df.write.format("delta").mode(mode).option("overwriteSchema", "true")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.saveAsTable(table_name)
    return df.count()


def optimize_delta_table(
    spark: SparkSession,
    table_name: str,
    z_order_columns: list[str] | None = None,
) -> None:
    z_cols = ", ".join(z_order_columns) if z_order_columns else ""
    if z_cols:
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({z_cols})")
    else:
        spark.sql(f"OPTIMIZE {table_name}")


def vacuum_delta_table(spark: SparkSession, table_name: str, retention_hours: int = 168) -> None:
    spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
