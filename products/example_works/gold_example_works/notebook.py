# Databricks notebook source
"""Aggregate clean Silver works into analytics-ready Gold metrics."""

from os import getenv

from lakehouse_engine.engine import load_data
from pyspark.sql import functions as F

CATALOG = getenv("EXAMPLE_WORKS_CATALOG", "dev_lakehouse")
DQ_ROOT = getenv("EXAMPLE_WORKS_DQ_ROOT", "/tmp/example_works/dq")
PREVIEW = getenv("EXAMPLE_WORKS_PREVIEW", "true").lower() == "true"
SILVER_TABLE = f"{CATALOG}.silver_example_works.works"
GOLD_TABLE = f"{CATALOG}.gold_example_works.category_summary"

READ_ACON = {
    "input_specs": [
        {
            "spec_id": "silver_table",
            "read_type": "batch",
            "data_format": "delta",
            "db_table": SILVER_TABLE,
        }
    ],
    "output_specs": [
        {
            "spec_id": "silver_works",
            "input_id": "silver_table",
            "data_format": "dataframe",
        }
    ],
}

if __name__ == "__main__":
    df_silver = load_data(acon=READ_ACON)["silver_works"]

    df_enriched = (
        df_silver.withColumn(
            "publication_decade", (F.floor(F.col("publication_year") / 10) * 10).cast("int")
        )
        .withColumn("tag_count", F.size("tags"))
        .withColumn("has_price", F.col("price").isNotNull())
    )

    df_gold = (
        df_enriched.groupBy("category", "publication_decade")
        .agg(
            F.count("work_id").cast("long").alias("work_count"),
            F.countDistinct("author_id").cast("long").alias("author_count"),
            F.sum("tag_count").cast("long").alias("tag_count"),
            F.sum(F.col("has_price").cast("int")).cast("long").alias("priced_work_count"),
            F.round(F.avg("price"), 2).alias("average_price"),
            F.round(F.avg("rating"), 2).alias("average_rating"),
            F.min("publication_year").alias("first_publication_year"),
            F.max("publication_year").alias("latest_publication_year"),
        )
        .withColumn("refreshed_at", F.current_timestamp())
        .orderBy("category", "publication_decade")
    )

    if PREVIEW:
        df_gold.show(20, truncate=False)

    load_data(
        acon={
            "input_specs": [
                {
                    "spec_id": "category_summary",
                    "read_type": "batch",
                    "data_format": "dataframe",
                    "df_name": df_gold,
                }
            ],
            "dq_specs": [
                {
                    "spec_id": "gold_quality",
                    "input_id": "category_summary",
                    "dq_type": "validator",
                    "store_backend": "file_system",
                    "local_fs_root_dir": f"{DQ_ROOT}/gold",
                    "unexpected_rows_pk": ["category", "publication_decade"],
                    "fail_on_error": True,
                    "dq_functions": [
                        {
                            "function": "expect_column_values_to_not_be_null",
                            "args": {"column": "category"},
                        },
                        {
                            "function": "expect_column_values_to_be_between",
                            "args": {"column": "work_count", "min_value": 1},
                        },
                    ],
                }
            ],
            "output_specs": [
                {
                    "spec_id": "gold_output",
                    "input_id": "gold_quality",
                    "write_type": "overwrite",
                    "data_format": "delta",
                    "db_table": GOLD_TABLE,
                    "options": {"overwriteSchema": "true"},
                }
            ],
        }
    )
