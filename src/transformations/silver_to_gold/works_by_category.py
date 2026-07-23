"""Silver -> Gold: an analytics data product.

Gold is built only from silver and is a deterministic rebuild — so it can be
recreated any time without re-calling the source API. This product aggregates
works into counts per category and year for a dashboard tile.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.schemas.gold.works_by_category import TABLE as GOLD


def run(spark: SparkSession, catalog: str = "dev_lakehouse") -> DataFrame:
    works = spark.table(f"{catalog}.silver.works")
    product = works.groupBy("category", "year").agg(F.count("*").alias("work_count"))

    # overwrite: a full, deterministic rebuild from silver
    (product.write.mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(f"{catalog}.{GOLD}"))
    return product
