"""Gold layer package."""

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

__all__ = [
    "build_activity_scores",
    "build_customer_lifetime_value",
    "build_customer_segmentation",
    "build_monthly_growth",
    "build_revenue_metrics",
    "optimize_delta_table",
    "vacuum_delta_table",
    "write_gold_table",
]
