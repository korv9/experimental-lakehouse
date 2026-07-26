# Databricks notebook source
"""SILVER | Gutenberg Bronze -> normalized Gutenberg works."""
from __future__ import annotations

from typing import Any

from pyspark.sql import Window
from pyspark.sql import functions as F

from products.philosophy_litterature.notebooks import _runtime

_runtime.bootstrap()

from lakehouse_platform.io.readers import uc_read
from lakehouse_platform.jobs import process_job
from products.philosophy_litterature.tables.silver.gutenberg_work.contract import (
    TableDefinition,
)


def split_values(column):
    values = F.transform(
        F.split(F.coalesce(column, F.lit("")), ";"),
        lambda item: F.trim(item),
    )
    return F.filter(values, lambda item: F.length(item) > 0)


def build_silver_gutenberg(df_bronze):
    """Normalize the latest version of every source-faithful Gutenberg row."""
    # Step 1: extract the catalog fields stored in Bronze raw_payload.
    df_1_parsed = df_bronze.select(
        "*",
        F.json_tuple(
            "raw_payload",
            "Text#",
            "Type",
            "Issued",
            "Title",
            "Language",
            "Authors",
            "Subjects",
            "LoCC",
            "Bookshelves",
        ).alias(
            "raw_gutenberg_id",
            "raw_media_type",
            "raw_issued",
            "raw_title",
            "raw_languages",
            "raw_authors",
            "raw_subjects",
            "raw_locc",
            "raw_bookshelves",
        ),
    )

    # Step 2: rank snapshots so the newest row for each Gutenberg ID is first.
    latest_snapshot = Window.partitionBy("raw_gutenberg_id").orderBy(
        F.col("source_snapshot_date").desc(),
        F.col("ingested_at").desc(),
    )
    df_2_ranked = df_1_parsed.select(
        "*",
        F.row_number().over(latest_snapshot).alias("snapshot_rank"),
    )

    # Step 3: keep only the current version of each catalog work.
    df_3_latest = df_2_ranked.filter(F.col("snapshot_rank") == 1)

    # Step 4: apply Silver names and types in one explicit projection.
    df_4_silver = df_3_latest.select(
        F.col("raw_gutenberg_id").alias("gutenberg_id"),
        F.col("raw_media_type").alias("media_type"),
        F.to_date("raw_issued").alias("issued_date"),
        F.trim("raw_title").alias("title"),
        split_values(F.col("raw_languages")).alias("language_codes"),
        split_values(F.col("raw_authors")).alias("authors"),
        split_values(F.col("raw_subjects")).alias("subjects"),
        split_values(F.col("raw_locc")).alias("locc_classes"),
        split_values(F.col("raw_bookshelves")).alias("bookshelves"),
        F.concat(
            F.lit("https://www.gutenberg.org/ebooks/"),
            F.col("raw_gutenberg_id"),
        ).alias("landing_page_url"),
        F.col("source_snapshot_date"),
        F.col("source_checksum"),
        F.col("source_file"),
        F.col("ingested_at"),
    )

    return df_4_silver


def main(spark_session: Any | None = None) -> str:
    spark_session = _runtime.active_spark(spark_session)
    catalog = _runtime.parameter("catalog", "dev_lakehouse", globals().get("dbutils"))

    df_bronze = uc_read(
        spark_session,
        "bronze.gutenberg_catalog_raw",
        catalog=catalog,
    )
    df_silver = build_silver_gutenberg(df_bronze)

    job_config = {
        "pipeline_name": "silver_gutenberg",
        "source_name": "project_gutenberg_catalog",
        "contract": TableDefinition,
        "expectations": {"min_rows": 1},
        "target": {
            "path": TableDefinition.object_location(),
            "format": "delta",
            "mode": "merge",
            "keys": ["gutenberg_id"],
            "when_matched": "update",
        },
    }

    return process_job(
        spark_session,
        job_config,
        catalog=catalog,
        dataframe=df_silver,
    )


if __name__ == "__main__":
    main()
