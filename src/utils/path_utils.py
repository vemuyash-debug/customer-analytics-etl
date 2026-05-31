"""ADLS and Unity Catalog path helpers."""

from __future__ import annotations

from datetime import date, timedelta


def build_adls_path(account: str, container: str, *parts: str) -> str:
    path = "/".join(part.strip("/") for part in parts if part)
    return f"abfss://{container}@{account}.dfs.core.windows.net/{path}"


def landing_partition_path(base_landing_path: str, process_date: date) -> str:
    return f"{base_landing_path.rstrip('/')}/year={process_date.year}/month={process_date.month:02d}/day={process_date.day:02d}"


def incremental_filter_date(lookback_days: int, anchor: date | None = None) -> date:
    anchor = anchor or date.today()
    return anchor - timedelta(days=lookback_days)
