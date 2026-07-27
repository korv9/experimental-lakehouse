"""Bronze -> Silver for the example source.

Domain logic only. Reading, the quality gate and the write are declared in
``pipelines/bronze_to_silver.yaml`` and executed by the platform, so this module
is a pure DataFrame-in, DataFrame-out step:

  1. _parse_and_flatten : parse raw_payload against RAW_WORK -> schema enforcement
  2. _dedupe_latest     : keep the latest row per business key

Quality (quarantining rows that fail an error rule) is applied by the ACON
engine using ``quality.yaml`` in this directory.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lakehouse_platform.observability.progress import progress
from products.example_works.tables.silver.works.contract import BUSINESS_KEY, RAW_WORK


def _parse_and_flatten(df: DataFrame) -> DataFrame:
    # from_json enforces the schema: bad/missing fields -> null, extras dropped
    parsed = df.withColumn("p", F.from_json("raw_payload", RAW_WORK))
    return parsed.select(
        F.col("p.id").alias("work_id"),
        F.col("p.title").alias("title"),
        F.col("p.category").alias("category"),
        F.col("p.year").alias("year"),
        F.col("p.language").alias("language"),
        F.col("p.tags").alias("tags"),
        F.col("p.author.id").alias("author_id"),
        F.col("p.author.name").alias("author_name"),
        F.to_timestamp("p.updated_at").alias("updated_at"),
        F.col("ingested_at"),
    )


def _dedupe_latest(df: DataFrame, key: str) -> DataFrame:
    # one row per business key: the most-recently-ingested version wins
    w = Window.partitionBy(key).orderBy(F.col("ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def transform(bronze: DataFrame, options: dict | None = None) -> DataFrame:
    """Pure ACON entrypoint: raw Bronze rows to deduplicated Works rows."""
    progress("EXAMPLE_WORKS", "Parsing and deduplicating Bronze works")
    result = _dedupe_latest(_parse_and_flatten(bronze), BUSINESS_KEY)
    progress("EXAMPLE_WORKS", "Silver Works transformation graph created")
    return result
