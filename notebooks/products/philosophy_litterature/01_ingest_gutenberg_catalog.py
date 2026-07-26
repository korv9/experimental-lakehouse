# Databricks notebook source
"""Workflow task 1: official Gutenberg catalog -> UC Volume -> Bronze."""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path
from typing import Any


def _repository_root() -> Path:
    start = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
    for candidate in (start, *start.parents):
        if (candidate / "pyproject.toml").exists() and (candidate / "src").is_dir():
            return candidate
    raise RuntimeError("Run this notebook from the checked-out Databricks Git folder")


REPOSITORY_ROOT = _repository_root()
for import_root in (REPOSITORY_ROOT / "src", REPOSITORY_ROOT):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from products.philosophy_litterature.tables.bronze.gutenberg_catalog_raw.transform import (
    run,
)

SOURCE_CONFIG = REPOSITORY_ROOT / "config" / "sources" / "gutenberg_catalog.yaml"


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
            raise RuntimeError("Attach Unity Catalog-enabled compute")
    catalog = _parameter("catalog", "dev_lakehouse")
    snapshot_value = _parameter("snapshot_date", "").strip()
    snapshot_date = date.fromisoformat(snapshot_value) if snapshot_value else None
    print("=" * 88)
    print("TASK 1 — OFFICIAL GUTENBERG CATALOG TO BRONZE")
    print("=" * 88)
    print(f"Source config: {SOURCE_CONFIG}")
    print(f"Catalog:       {catalog}")
    print(f"Snapshot:      {snapshot_date or 'current UTC date'}")
    print(f"Landing:       /Volumes/{catalog}/landing/source_files/gutenberg/catalog/...")
    print(f"Target:        {catalog}.bronze.gutenberg_catalog_raw")
    run_id = run(
        spark_session,
        SOURCE_CONFIG,
        catalog=catalog,
        snapshot_date=snapshot_date,
    )
    rows = spark_session.table(f"{catalog}.bronze.gutenberg_catalog_raw").count()
    print(f"[DONE] run_id={run_id} bronze_rows={rows}")
    return run_id


if __name__ == "__main__":
    main()
