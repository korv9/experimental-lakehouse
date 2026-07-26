"""Shared Databricks runtime bootstrap kept out of transformation notebooks."""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


def bootstrap() -> None:
    for import_root in (REPOSITORY_ROOT / "src", REPOSITORY_ROOT):
        if str(import_root) not in sys.path:
            sys.path.insert(0, str(import_root))


def active_spark(spark_session: Any | None = None):
    if spark_session is not None:
        return spark_session

    from pyspark.sql import SparkSession

    spark_session = SparkSession.getActiveSession()
    if spark_session is None:
        raise RuntimeError("Attach Unity Catalog-enabled compute")
    return spark_session


def parameter(name: str, default: str, dbutils_object: Any | None = None) -> str:
    if dbutils_object is None:
        return os.environ.get(name.upper(), default)
    dbutils_object.widgets.text(name, default)
    return str(dbutils_object.widgets.get(name))
