"""Reproducible experimental aggregations over the Kimball model."""
from __future__ import annotations

from collections import defaultdict

from lakehouse_platform.observability.progress import progress


def aggregate_category_metrics(
    fact_work: list[dict],
    dim_category: list[dict],
) -> list[dict]:
    """Aggregate work and tag counts by category using fact/dimension keys."""
    progress("EXPERIMENT", "Aggregating category metrics")
    category_by_key = {
        row["category_key"]: row["category_name"] for row in dim_category
    }
    totals = defaultdict(lambda: {"work_count": 0, "tag_count": 0})
    for row in fact_work:
        category = category_by_key[row["category_key"]]
        totals[category]["work_count"] += row["work_count"]
        totals[category]["tag_count"] += row["tag_count"]

    result = [
        {"category": category, **metrics}
        for category, metrics in sorted(totals.items())
    ]
    progress("EXPERIMENT", "Aggregation completed", rows=len(result))
    return result
