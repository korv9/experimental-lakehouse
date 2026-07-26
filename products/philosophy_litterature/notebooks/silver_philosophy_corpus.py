# Databricks notebook source
"""SILVER | Normalized Gutenberg works -> reviewed Philosophy corpus."""
from __future__ import annotations

from typing import Any

from pyspark.sql import functions as F
from pyspark.sql import types as T

from products.philosophy_litterature.notebooks import _runtime

_runtime.bootstrap()

from lakehouse_platform.io.readers import uc_read
from lakehouse_platform.jobs import process_job
from products.philosophy_litterature.selection import load_selection
from products.philosophy_litterature.tables.silver.philosophy_litterature_work.contract import (
    TableDefinition,
)

REPORT = _runtime.REPOSITORY_ROOT / "datasets" / "api_samples" / "philosophy_corpus_report.json"
SELECTION_SCHEMA = T.StructType(
    [
        T.StructField("corpus_id", T.StringType(), False),
        T.StructField("corpus_work_id", T.StringType(), False),
        T.StructField("gutenberg_id", T.StringType(), False),
        T.StructField("period", T.StringType(), False),
        T.StructField("canonical_author", T.StringType(), False),
        T.StructField("canonical_title", T.StringType(), False),
        T.StructField("match_status", T.StringType(), False),
        T.StructField("text_url", T.StringType(), True),
    ]
)


def build_silver_philosophy_corpus(catalog_works, selection):
    return selection.alias("selection").join(
        catalog_works.alias("catalog"),
        "gutenberg_id",
        "inner",
    ).select(
        F.col("selection.corpus_id"),
        F.col("selection.corpus_work_id"),
        F.col("gutenberg_id"),
        F.col("selection.period"),
        F.col("selection.canonical_author"),
        F.col("selection.canonical_title"),
        F.col("selection.match_status"),
        F.col("catalog.title"),
        F.col("catalog.language_codes"),
        F.col("catalog.authors"),
        F.col("catalog.subjects"),
        F.col("catalog.locc_classes"),
        F.col("catalog.bookshelves"),
        F.col("selection.text_url"),
        F.col("catalog.landing_page_url"),
        F.col("catalog.source_snapshot_date"),
        F.col("catalog.source_checksum"),
        F.col("catalog.ingested_at"),
    )


def main(spark_session: Any | None = None) -> str:
    spark_session = _runtime.active_spark(spark_session)
    catalog = _runtime.parameter("catalog", "dev_lakehouse", globals().get("dbutils"))
    selection_rows = load_selection(REPORT)
    selection = spark_session.createDataFrame(selection_rows, schema=SELECTION_SCHEMA)

    catalog_works = uc_read(
        spark_session,
        "silver.gutenberg_work",
        catalog=catalog,
    )
    philosophy_work = build_silver_philosophy_corpus(catalog_works, selection)

    job_config = {
        "pipeline_name": "silver_philosophy_corpus",
        "source_name": "project_gutenberg_catalog",
        "contract": TableDefinition,
        "expectations": {
            "row_count": len(selection_rows),
            "array_contains": {"language_codes": "en"},
        },
        "target": {
            "path": TableDefinition.object_location(),
            "format": "delta",
            "mode": "merge",
            "keys": ["corpus_work_id"],
            "when_matched": "update",
        },
    }

    return process_job(
        spark_session,
        job_config,
        catalog=catalog,
        dataframe=philosophy_work,
    )


if __name__ == "__main__":
    main()
