# Databricks notebook source
"""Gutenberg Bronze -> normalized Gutenberg Silver."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

REPOSITORY_ROOT = (
    Path(__file__).resolve().parents[3] if "__file__" in globals() else Path.cwd()
)
for import_root in (REPOSITORY_ROOT / "src", REPOSITORY_ROOT):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from lakehouse_platform.io.readers import uc_read
from lakehouse_platform.jobs import JobContext, process_job
from products.philosophy_litterature.tables.silver.gutenberg_work.contract import (
    TableDefinition,
)

RAW_CATALOG = T.StructType(
    [
        T.StructField("Text#", T.StringType()),
        T.StructField("Type", T.StringType()),
        T.StructField("Issued", T.StringType()),
        T.StructField("Title", T.StringType()),
        T.StructField("Language", T.StringType()),
        T.StructField("Authors", T.StringType()),
        T.StructField("Subjects", T.StringType()),
        T.StructField("LoCC", T.StringType()),
        T.StructField("Bookshelves", T.StringType()),
    ]
)


def split_values(column: F.Column) -> F.Column:
    values = F.transform(
        F.split(F.coalesce(column, F.lit("")), ";"),
        lambda item: F.trim(item),
    )
    return F.filter(values, lambda item: F.length(item) > 0)


def build_gutenberg_work(bronze: DataFrame) -> DataFrame:
    parsed = bronze.withColumn("record", F.from_json("raw_payload", RAW_CATALOG))
    latest_snapshot = Window.partitionBy(F.col("record.`Text#`")).orderBy(
        F.col("source_snapshot_date").desc(),
        F.col("ingested_at").desc(),
    )
    return (
        parsed.withColumn("snapshot_rank", F.row_number().over(latest_snapshot))
        .filter(F.col("snapshot_rank") == 1)
        .select(
            F.col("record.`Text#`").alias("gutenberg_id"),
            F.col("record.Type").alias("media_type"),
            F.to_date(F.col("record.Issued")).alias("issued_date"),
            F.trim(F.col("record.Title")).alias("title"),
            split_values(F.col("record.Language")).alias("language_codes"),
            split_values(F.col("record.Authors")).alias("authors"),
            split_values(F.col("record.Subjects")).alias("subjects"),
            split_values(F.col("record.LoCC")).alias("locc_classes"),
            split_values(F.col("record.Bookshelves")).alias("bookshelves"),
            F.concat(
                F.lit("https://www.gutenberg.org/ebooks/"),
                F.col("record.`Text#`"),
            ).alias("landing_page_url"),
            F.col("source_snapshot_date"),
            F.col("source_checksum"),
            F.col("source_file"),
            F.col("ingested_at"),
        )
    )


def validate_gutenberg_work(result: DataFrame) -> None:
    missing_title = result.filter(
        F.col("title").isNull() | (F.length(F.col("title")) == 0)
    ).limit(1)
    if missing_title.count():
        raise ValueError("Normalized Gutenberg catalog contains a work without a title")


def _catalog(default: str = "dev_lakehouse") -> str:
    dbutils_object: Any = globals().get("dbutils")
    if dbutils_object is None:
        return default
    dbutils_object.widgets.text("catalog", default)
    return str(dbutils_object.widgets.get("catalog"))


def main(spark_session: Any | None = None) -> str:
    if spark_session is None:
        from pyspark.sql import SparkSession

        spark_session = SparkSession.getActiveSession()
        if spark_session is None:
            raise RuntimeError("Attach Unity Catalog-enabled compute")

    catalog = _catalog()

    def build_table(context: JobContext) -> DataFrame:
        bronze = uc_read(
            context.spark,
            "bronze.gutenberg_catalog_raw",
            catalog=context.catalog,
        )
        return build_gutenberg_work(bronze)

    print("=" * 88)
    print("NORMALIZE GUTENBERG CATALOG")
    print(f"Source: {catalog}.bronze.gutenberg_catalog_raw")
    print(f"Target: {catalog}.{TableDefinition.object_location()}")

    job_config = {
        "pipeline_name": "gutenberg_catalog_bronze_to_silver",
        "source_name": "project_gutenberg_catalog",
        "target": {
            "path": TableDefinition.object_location(),
            "format": "delta",
            "mode": "merge",
            "keys": ["gutenberg_id"],
            "when_matched": "update",
        },
        "transformation": build_table,
        "validation": {
            "contract": TableDefinition,
            "checks": [validate_gutenberg_work],
        },
    }

    return process_job(spark_session, job_config, catalog=catalog)


if __name__ == "__main__":
    main()
