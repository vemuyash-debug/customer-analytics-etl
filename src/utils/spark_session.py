"""Spark session factory with Delta Lake and performance defaults."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "CustomerAnalyticsETL") -> SparkSession:
    from pyspark.sql import SparkSession

    config_path = Path(__file__).resolve().parents[2] / "config" / "spark_config.conf"
    builder = SparkSession.builder.appName(app_name)

    if config_path.exists():
        with config_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if " " in line:
                    key, value = line.split(None, 1)
                    builder = builder.config(key.strip(), value.strip())

    return builder.getOrCreate()


def set_catalog_context(spark: SparkSession, catalog: str, schema: str | None = None) -> None:
    spark.sql(f"USE CATALOG {catalog}")
    if schema:
        spark.sql(f"USE SCHEMA {schema}")
