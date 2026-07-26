# Databricks notebook source
"""Normalized Gutenberg catalog -> reviewed Philosophy corpus Silver."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
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
from products.philosophy_litterature.selection import load_selection
from products.philosophy_litterature.tables.silver.philosophy_litterature_work.contract import (
    TableDefinition,
)

REPORT = REPOSITORY_ROOT / "datasets" / "api_samples" / "philosophy_corpus_report.json"
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


def build_philosophy_work(catalog_works: DataFrame, selection: DataFrame) -> DataFrame:
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


def validate_philosophy_work(result: DataFrame, expected_work_ids: set[str]) -> None:
    found_work_ids = {
        row["corpus_work_id"] for row in result.select("corpus_work_id").collect()
    }
    missing_work_ids = sorted(expected_work_ids - found_work_ids)
    if missing_work_ids:
        raise ValueError(
            f"Official catalog is missing {len(missing_work_ids)} approved corpus works: "
            f"{missing_work_ids}"
        )
    has_non_english = result.filter(
        ~F.array_contains(F.col("language_codes"), "en")
    ).limit(1)
    if has_non_english.count():
        raise ValueError("Approved English-language corpus contains a non-English row")


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
    selection_rows = load_selection(REPORT)
    expected_work_ids = {row["corpus_work_id"] for row in selection_rows}
    selection = spark_session.createDataFrame(selection_rows, schema=SELECTION_SCHEMA)

    def build_table(context: JobContext) -> DataFrame:
        catalog_works = uc_read(
            context.spark,
            "silver.gutenberg_work",
            catalog=context.catalog,
        )
        return build_philosophy_work(catalog_works, selection)

    print("=" * 88)
    print("SELECT APPROVED PHILOSOPHY CORPUS")
    print(f"Approved: {len(selection_rows)} corpus works")
    print(f"Source:   {catalog}.silver.gutenberg_work")
    print(f"Target:   {catalog}.{TableDefinition.object_location()}")

    job_config = {
        "pipeline_name": "select_philosophy_corpus",
        "source_name": "project_gutenberg_catalog",
        "target": {
            "path": TableDefinition.object_location(),
            "format": "delta",
            "mode": "merge",
            "keys": ["corpus_work_id"],
            "when_matched": "update",
        },
        "transformation": build_table,
        "validation": {
            "contract": TableDefinition,
            "checks": [
                lambda result: validate_philosophy_work(result, expected_work_ids)
            ],
        },
    }

    return process_job(spark_session, job_config, catalog=catalog)


if __name__ == "__main__":
    main()
