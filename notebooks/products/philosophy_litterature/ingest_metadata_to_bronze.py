# Databricks notebook source
"""Job entrypoint: approved Philosophy corpus metadata -> Unity Catalog Bronze."""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any


def _repository_root() -> Path:
    start = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
    for candidate in (start, *start.parents):
        if (candidate / "pyproject.toml").exists() and (candidate / "src").is_dir():
            return candidate
    raise RuntimeError(
        "Could not find the repository root. Run this file from the checked-out "
        "experimental-lakehouse Databricks Git folder."
    )


REPOSITORY_ROOT = _repository_root()
SOURCE_ROOT = REPOSITORY_ROOT / "src"
for import_root in (SOURCE_ROOT, REPOSITORY_ROOT):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from lakehouse_platform.ingestion.runner import ingest_corpus
from lakehouse_platform.metadata.unity_catalog import UnityCatalogLayout

SOURCE_CONFIG = REPOSITORY_ROOT / "config" / "sources" / "philosophy_gutendex.yaml"
BRONZE_TABLE = "bronze.philosophy_litterature_work_raw"


def _parameter(name: str, default: str) -> str:
    dbutils_object: Any = globals().get("dbutils")
    if dbutils_object is not None:
        dbutils_object.widgets.text(name, default)
        return str(dbutils_object.widgets.get(name))
    return os.environ.get(name.upper(), default)


def main(spark_session: Any | None = None) -> str:
    if spark_session is None:
        from pyspark.sql import SparkSession

        spark_session = SparkSession.getActiveSession()
        if spark_session is None:
            raise RuntimeError("No active Spark session. Attach Unity Catalog-enabled compute.")

    catalog = _parameter("catalog", "dev_lakehouse")
    UnityCatalogLayout(catalog)  # validates the catalog identifier before SQL is generated
    target = f"{catalog}.{BRONZE_TABLE}"

    print("=" * 88)
    print("PHILOSOPHY BOOKS — APPROVED CORPUS METADATA TO BRONZE")
    print("=" * 88)
    print(f"Repository root: {REPOSITORY_ROOT}")
    print(f"Source config:   {SOURCE_CONFIG}")
    print(f"Target table:    {target}")
    print("Selection:       matched + matched_without_plain_text")
    print("Write strategy:  Delta MERGE on deterministic ingestion_id")

    before = spark_session.table(target).count() if spark_session.catalog.tableExists(target) else 0
    print(f"Bronze rows before run: {before}")

    run_id = ingest_corpus(
        spark_session,
        str(SOURCE_CONFIG),
        catalog=catalog,
    )

    after = spark_session.table(target).count()
    distinct_books = (
        spark_session.table(target).select("source_record_id").distinct().count()
    )
    print()
    print("=" * 88)
    print("INGESTION SUMMARY")
    print("=" * 88)
    print(f"Run ID:                  {run_id}")
    print(f"Bronze rows before:      {before}")
    print(f"Bronze rows after:       {after}")
    print(f"New raw versions:        {after - before}")
    print(f"Distinct Gutenberg IDs:  {distinct_books}")
    print(f"Audit table:             {catalog}.platform.pipeline_runs")
    print(f"Checkpoint table:        {catalog}.platform.ingestion_checkpoints")
    print("Full-text files are not downloaded by this metadata job.")
    return run_id


if __name__ == "__main__":
    main()
