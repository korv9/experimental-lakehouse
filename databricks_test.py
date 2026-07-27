# Databricks notebook source
"""RUN THIS to test the platform on Databricks.

Exercises the messy_records product end to end — land the seed into Bronze,
clean it into Silver through the quality and contract gates, then verify the
row counts, the run metadata and the persisted quality results. No external API
is involved, so it works on a fresh workspace.

Widgets: ``catalog`` (default dev_lakehouse), ``create_catalog``,
``setup_platform``. It raises on failure, so a job task turns red.

Works as a notebook, a Python file task, or a checked-out Repo file: the
repository's ``src`` directory is put on ``sys.path`` so an editable install is
not required for this first run.
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

from lakehouse_platform.tools.databricks_demo import parse_bool  # noqa: E402
from lakehouse_platform.tools.databricks_test import (  # noqa: E402
    DatabricksTestOptions,
    run_databricks_test,
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
                "No active Spark session. Run databricks_test.py on Databricks "
                "compute with Unity Catalog enabled."
            )

    options = DatabricksTestOptions(
        catalog=_parameter("catalog", "dev_lakehouse"),
        repository_root=REPOSITORY_ROOT,
        create_catalog=parse_bool(_parameter("create_catalog", "true")),
        setup_platform=parse_bool(_parameter("setup_platform", "true")),
    )
    return run_databricks_test(spark_session, options)


if __name__ == "__main__":
    main()
