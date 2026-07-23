"""Declarative Bronze -> Silver -> Gold with Delta Live Tables (DLT / Lakeflow).

WHY DLT HERE: for the medallion transform, DLT is the most efficient option — it
manages incremental processing, table creation, dependency ordering and retries
for you, and ``@dlt.expect_*`` gives inline data-quality gates. (API ingestion
stays imperative in pipelines/ingestion/ because DLT can't drive paginated REST
calls.)

Two data-quality options, shown for contrast:
  * native DLT expectations (used below) — lightweight, inline, zero extra deps
  * DQX (src/quality/dqx_checks.py) — richer, reusable, config-driven rules that
    can also be applied *inside* a DLT table function

``spark`` and ``dlt`` are provided by the DLT runtime; there is no main().
"""
import dlt
from pyspark.sql import functions as F

from src.schemas.silver.works import RAW_WORK


@dlt.table(comment="Raw, append-only example records (landed JSON)")
def bronze_example_data():
    # Auto Loader incrementally ingests new files from the landing volume
    return (spark.readStream.format("cloudFiles")          # noqa: F821 (spark is provided)
            .option("cloudFiles.format", "json")
            .load("/Volumes/dev/landing/example_data/"))


@dlt.table(comment="Cleaned, schema-enforced works")
@dlt.expect_or_drop("valid_work_id", "work_id IS NOT NULL")   # error rule: drop bad rows
@dlt.expect("reasonable_year", "year BETWEEN 0 AND 2100")     # warn rule: keep + flag
def silver_works():
    parsed = dlt.read_stream("bronze_example_data").withColumn(
        "p", F.from_json("raw_payload", RAW_WORK))            # schema enforcement
    return parsed.select(
        F.col("p.id").alias("work_id"),
        F.col("p.title").alias("title"),
        F.col("p.category").alias("category"),
        F.col("p.year").alias("year"),
    ).dropDuplicates(["work_id"])


@dlt.table(comment="Works per category/year — dashboard product")
def gold_analytics_works_by_category():
    return (dlt.read("silver_works")
            .groupBy("category", "year")
            .agg(F.count("*").alias("work_count")))
