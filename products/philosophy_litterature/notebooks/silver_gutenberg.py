# Databricks notebook source
"""SILVER | Gutenberg Bronze -> normalized Gutenberg works."""
from __future__ import annotations

from typing import Any

from products.philosophy_litterature.notebooks import _runtime

_runtime.bootstrap()

from lakehouse_platform.jobs import process_job  # noqa: E402

ACON = _runtime.REPOSITORY_ROOT / "products" / "philosophy_litterature" / "pipelines" / (
    "silver_gutenberg.yaml"
)


def main(spark_session: Any | None = None) -> str:
    spark_session = _runtime.active_spark(spark_session)
    catalog = _runtime.parameter("catalog", "dev_lakehouse", globals().get("dbutils"))
    return process_job(spark_session, acon=ACON, catalog=catalog)


if __name__ == "__main__":
    main()
