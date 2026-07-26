# Databricks notebook source
"""Workflow task 3: normalized catalog -> approved Philosophy corpus."""
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
    raise RuntimeError("Run this notebook from the checked-out Databricks Git folder")


REPOSITORY_ROOT = _repository_root()
for import_root in (REPOSITORY_ROOT / "src", REPOSITORY_ROOT):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from products.philosophy_litterature.tables.silver.philosophy_litterature_work.transform import (
    run,
)

REPORT = REPOSITORY_ROOT / "datasets" / "api_samples" / "philosophy_corpus_report.json"


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
    print("=" * 88)
    print("TASK 3 — SELECT APPROVED PHILOSOPHY CORPUS")
    print("=" * 88)
    print(f"Selection: {REPORT}")
    print(f"Source:    {catalog}.silver.gutenberg_work")
    print(f"Target:    {catalog}.silver.philosophy_litterature_work")
    run_id = run(spark_session, REPORT, catalog=catalog)
    rows = spark_session.table(f"{catalog}.silver.philosophy_litterature_work").count()
    print(f"[DONE] run_id={run_id} approved_corpus_rows={rows}")
    return run_id


if __name__ == "__main__":
    main()
