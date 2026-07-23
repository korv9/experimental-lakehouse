"""Serving/export: a small Gold product -> JSON for the portfolio site.

The portfolio reads a tiny cached JSON file instead of querying the lakehouse on
every page load. Only a single small gold table is read here, then written to the
top-level ``exports/`` tree (durable code lives in src/, emitted artifacts in
exports/).
"""
from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def export_featured_works(spark: SparkSession, catalog: str = "dev_lakehouse",
                          out_path: str = "exports/portfolio/featured_works.json",
                          limit: int = 20) -> str:
    gold = f"{catalog}.gold.analytics_works_by_category"
    rows = (spark.table(gold)
            .orderBy(F.col("work_count").desc())
            .limit(limit)
            .toPandas()
            .to_dict(orient="records"))

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_path).write_text(json.dumps(rows, indent=2, default=str))
    return out_path
