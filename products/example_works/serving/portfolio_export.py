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
    fact = spark.table(f"{catalog}.gold.fact_work")
    category = spark.table(f"{catalog}.gold.dim_category")
    work = spark.table(f"{catalog}.gold.dim_work")
    rows = (
        fact.join(category, "category_key")
        .join(work, "work_key")
        .groupBy(F.col("category_name").alias("category"), "year")
        .agg(F.sum("work_count").alias("work_count"))
        .orderBy(F.col("work_count").desc(), "category", "year")
        .limit(limit)
        .toPandas()
        .to_dict(orient="records")
    )

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(out_path).write_text(json.dumps(rows, indent=2, default=str))
    print(f"[EXPORT] Wrote {len(rows)} portfolio rows to {out_path}")
    return out_path
