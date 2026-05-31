"""Structured pipeline logging utilities."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


AUDIT_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("step", StringType(), False),
        StructField("status", StringType(), False),
        StructField("records_processed", LongType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("message", StringType(), True),
        StructField("metadata_json", StringType(), True),
        StructField("event_timestamp", TimestampType(), False),
    ]
)


class AuditLogger:
    """Writes pipeline execution audit records to a Delta table."""

    def __init__(self, spark: SparkSession, audit_table: str) -> None:
        self.spark = spark
        self.audit_table = audit_table
        self.run_id = str(uuid.uuid4())
        self.logger = get_logger("audit")

    def log_event(
        self,
        pipeline_name: str,
        layer: str,
        step: str,
        status: str,
        records_processed: int | None = None,
        duration_seconds: float | None = None,
        message: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        row = [
            (
                self.run_id,
                pipeline_name,
                layer,
                step,
                status,
                records_processed,
                duration_seconds,
                message,
                json.dumps(metadata or {}),
                datetime.now(timezone.utc),
            )
        ]
        df = self.spark.createDataFrame(row, schema=AUDIT_SCHEMA)
        df.write.format("delta").mode("append").saveAsTable(self.audit_table)
        self.logger.info(
            "%s | %s | %s | status=%s | records=%s",
            layer,
            step,
            pipeline_name,
            status,
            records_processed,
        )


def add_ingestion_metadata(
    df: DataFrame,
    source_system: str,
    ingestion_mode: str = "incremental",
) -> DataFrame:
    return (
        df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_ingestion_mode", F.lit(ingestion_mode))
        .withColumn("_batch_id", F.lit(str(uuid.uuid4())))
    )
