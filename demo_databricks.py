# Databricks notebook source
"""Run this first in Databricks to bootstrap and explain the platform.

The file works as a Databricks notebook, a Python file task, or a checked-out
Repo file. It adds the repository's ``src`` directory to ``sys.path`` so an
editable install is not required for this first diagnostic run.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any


def _repository_root() -> Path:
    if "__file__" in globals():
        return Path(__file__).resolve().parent
    return Path.cwd()


REPOSITORY_ROOT = _repository_root()
SOURCE_ROOT = REPOSITORY_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from lakehouse_platform.tools.databricks_demo import (
    DatabricksDemoOptions,
    parse_bool,
    run_databricks_demo,
)


def _parameter(name: str, default: str) -> str:
    dbutils_object: Any = globals().get("dbutils")
    if dbutils_object is not None:
        dbutils_object.widgets.text(name, default)
        return str(dbutils_object.widgets.get(name))
    return os.environ.get(name.upper(), default)


def main(spark_session: Any | None = None):
    if spark_session is None:
        from pyspark.sql import SparkSession

        spark_session = SparkSession.getActiveSession()
        if spark_session is None:
            raise RuntimeError(
                "No active Spark session. Run demo_databricks.py on Databricks "
                "compute with Unity Catalog enabled."
            )

    options = DatabricksDemoOptions(
        catalog=_parameter("catalog", "dev_lakehouse"),
        create_catalog=parse_bool(_parameter("create_catalog", "true")),
        run_volume_probe=parse_bool(_parameter("run_volume_probe", "true")),
        run_api_smoke=parse_bool(_parameter("run_api_smoke", "true")),
    )
    return run_databricks_demo(spark_session, options)


if __name__ == "__main__":
    main()
