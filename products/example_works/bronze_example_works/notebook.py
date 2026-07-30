# Databricks notebook source
"""Preserve the messy source response in a queryable Bronze table."""

from os import getenv
from pathlib import Path

from lakehouse_engine.engine import load_data
from pyspark.sql import functions as F

CATALOG = getenv("EXAMPLE_WORKS_CATALOG", "dev_lakehouse")
DQ_ROOT = getenv("EXAMPLE_WORKS_DQ_ROOT", "/tmp/example_works/dq")
PREVIEW = getenv("EXAMPLE_WORKS_PREVIEW", "true").lower() == "true"
SOURCE = getenv(
    "EXAMPLE_WORKS_SOURCE",
    (Path(__file__).resolve().parents[3] / "datasets/example_works/works.json").as_uri(),
)
BRONZE_TABLE = f"{CATALOG}.bronze_example_works.works"

READ_ACON = {
    "input_specs": [
        {
            "spec_id": "source_file",
            "read_type": "batch",
            "data_format": "json",
            "location": SOURCE,
            "options": {"multiLine": True},
        }
    ],
    "output_specs": [
        {
            "spec_id": "source_response",
            "input_id": "source_file",
            "data_format": "dataframe",
        }
    ],
}

if __name__ == "__main__":
    df_source = load_data(acon=READ_ACON)["source_response"]

    df_records = df_source.select(
        "extract_id",
        F.col("source.system").alias("source_system"),
        F.col("source.region").alias("source_region"),
        F.explode_outer("records").alias("record"),
    )

    df_bronze = df_records.select(
        F.sha2(F.to_json("record"), 256).alias("source_row_id"),
        "extract_id",
        "source_system",
        "source_region",
        F.col("record.id").cast("string").alias("raw_work_id"),
        F.col("record.title").cast("string").alias("raw_title"),
        F.col("record.author.id").cast("string").alias("raw_author_id"),
        F.col("record.author.name").cast("string").alias("raw_author_name"),
        F.col("record.category").cast("string").alias("raw_category"),
        F.col("record.year").cast("string").alias("raw_publication_year"),
        F.col("record.language").cast("string").alias("raw_language"),
        F.col("record.tags").alias("raw_tags"),
        F.col("record.price").cast("string").alias("raw_price"),
        F.col("record.rating").cast("string").alias("raw_rating"),
        F.col("record.status").cast("string").alias("raw_status"),
        F.col("record.updated_at").cast("string").alias("raw_updated_at"),
        F.to_json("record").alias("raw_payload"),
        F.current_timestamp().alias("ingested_at"),
    )

    if PREVIEW:
        df_bronze.show(20, truncate=False)

    load_data(
        acon={
            "input_specs": [
                {
                    "spec_id": "bronze_works",
                    "read_type": "batch",
                    "data_format": "dataframe",
                    "df_name": df_bronze,
                }
            ],
            "dq_specs": [
                {
                    "spec_id": "bronze_quality",
                    "input_id": "bronze_works",
                    "dq_type": "validator",
                    "store_backend": "file_system",
                    "local_fs_root_dir": f"{DQ_ROOT}/bronze",
                    "unexpected_rows_pk": ["source_row_id"],
                    "fail_on_error": True,
                    "dq_functions": [
                        {
                            "function": "expect_column_values_to_not_be_null",
                            "args": {"column": "source_row_id"},
                        },
                        {
                            "function": "expect_table_row_count_to_be_between",
                            "args": {"min_value": 1},
                        },
                    ],
                }
            ],
            "output_specs": [
                {
                    "spec_id": "bronze_output",
                    "input_id": "bronze_quality",
                    "write_type": "overwrite",
                    "data_format": "delta",
                    "db_table": BRONZE_TABLE,
                    "options": {"overwriteSchema": "true"},
                }
            ],
        }
    )
