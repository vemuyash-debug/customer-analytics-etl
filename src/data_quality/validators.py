"""Data quality validation framework for Medallion pipeline layers."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class CheckStatus(str, Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"


@dataclass
class QualityCheckResult:
    check_name: str
    status: CheckStatus
    metric_value: float | None = None
    threshold: float | None = None
    details: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


DQ_RESULT_SCHEMA = StructType(
    [
        StructField("check_id", StringType(), False),
        StructField("run_id", StringType(), True),
        StructField("table_name", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("metric_value", DoubleType(), True),
        StructField("threshold", DoubleType(), True),
        StructField("details", StringType(), True),
        StructField("metadata_json", StringType(), True),
        StructField("check_timestamp", TimestampType(), False),
    ]
)


class DataQualityValidator:
    """Runs null, duplicate, schema, and record-count validations."""

    def __init__(
        self,
        spark: SparkSession,
        audit_table: str,
        null_threshold_pct: float = 5.0,
        duplicate_threshold_pct: float = 1.0,
        min_record_count: int = 1,
    ) -> None:
        self.spark = spark
        self.audit_table = audit_table
        self.null_threshold_pct = null_threshold_pct
        self.duplicate_threshold_pct = duplicate_threshold_pct
        self.min_record_count = min_record_count

    def validate_nulls(
        self,
        df: DataFrame,
        required_columns: list[str],
    ) -> QualityCheckResult:
        total = df.count()
        if total == 0:
            return QualityCheckResult(
                check_name="null_check",
                status=CheckStatus.FAILED,
                details="Dataset is empty; cannot evaluate null percentages.",
            )

        null_counts = []
        for col_name in required_columns:
            if col_name not in df.columns:
                null_counts.append((col_name, 100.0))
                continue
            null_pct = df.filter(F.col(col_name).isNull()).count() / total * 100
            null_counts.append((col_name, null_pct))

        max_null_pct = max(pct for _, pct in null_counts)
        failed_cols = [c for c, pct in null_counts if pct > self.null_threshold_pct]
        status = CheckStatus.PASSED if not failed_cols else CheckStatus.FAILED
        return QualityCheckResult(
            check_name="null_check",
            status=status,
            metric_value=max_null_pct,
            threshold=self.null_threshold_pct,
            details=f"Columns exceeding threshold: {failed_cols}" if failed_cols else "All required columns within threshold.",
            metadata={"column_null_pcts": {c: round(p, 2) for c, p in null_counts}},
        )

    def validate_duplicates(
        self,
        df: DataFrame,
        key_columns: list[str],
    ) -> QualityCheckResult:
        total = df.count()
        if total == 0:
            return QualityCheckResult(
                check_name="duplicate_check",
                status=CheckStatus.WARNING,
                details="Empty dataset; duplicate check skipped.",
            )

        distinct = df.select(*key_columns).distinct().count()
        duplicate_pct = (total - distinct) / total * 100
        status = (
            CheckStatus.PASSED
            if duplicate_pct <= self.duplicate_threshold_pct
            else CheckStatus.FAILED
        )
        return QualityCheckResult(
            check_name="duplicate_check",
            status=status,
            metric_value=duplicate_pct,
            threshold=self.duplicate_threshold_pct,
            details=f"Found {total - distinct} duplicate records on keys {key_columns}.",
        )

    def validate_schema(
        self,
        df: DataFrame,
        expected_columns: list[str],
    ) -> QualityCheckResult:
        actual = set(df.columns)
        expected = set(expected_columns)
        missing = expected - actual
        status = CheckStatus.PASSED if not missing else CheckStatus.FAILED
        return QualityCheckResult(
            check_name="schema_validation",
            status=status,
            details="Schema matches expected columns."
            if not missing
            else f"Missing columns: {sorted(missing)}",
            metadata={"missing_columns": sorted(missing), "extra_columns": sorted(actual - expected)},
        )

    def validate_record_count(self, df: DataFrame) -> QualityCheckResult:
        count = df.count()
        status = CheckStatus.PASSED if count >= self.min_record_count else CheckStatus.FAILED
        return QualityCheckResult(
            check_name="record_count",
            status=status,
            metric_value=float(count),
            threshold=float(self.min_record_count),
            details=f"Record count: {count}",
        )

    def run_all_checks(
        self,
        df: DataFrame,
        table_name: str,
        layer: str,
        required_columns: list[str],
        key_columns: list[str],
        run_id: str | None = None,
    ) -> list[QualityCheckResult]:
        results = [
            self.validate_record_count(df),
            self.validate_schema(df, required_columns),
            self.validate_nulls(df, required_columns),
            self.validate_duplicates(df, key_columns),
        ]
        self._persist_results(results, table_name, layer, run_id)
        failures = [r for r in results if r.status == CheckStatus.FAILED]
        if failures:
            raise ValueError(
                f"Data quality checks failed for {table_name}: "
                + "; ".join(f"{r.check_name}: {r.details}" for r in failures)
            )
        return results

    def _persist_results(
        self,
        results: list[QualityCheckResult],
        table_name: str,
        layer: str,
        run_id: str | None,
    ) -> None:
        rows = [
            (
                str(uuid.uuid4()),
                run_id,
                table_name,
                layer,
                r.check_name,
                r.status.value,
                r.metric_value,
                r.threshold,
                r.details,
                json.dumps(r.metadata),
                datetime.now(timezone.utc),
            )
            for r in results
        ]
        dq_df = self.spark.createDataFrame(rows, schema=DQ_RESULT_SCHEMA)
        dq_df.write.format("delta").mode("append").saveAsTable(self.audit_table)
