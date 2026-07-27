# Databricks notebook source
"""SILVER | Normalized Gutenberg works -> reviewed Philosophy corpus."""
from __future__ import annotations

from typing import Any

from products.philosophy_litterature.notebooks import _runtime

_runtime.bootstrap()

from lakehouse_platform.jobs import process_job  # noqa: E402

ACON = _runtime.REPOSITORY_ROOT / "products" / "philosophy_litterature" / "pipelines" / (
    "silver_philosophy_corpus.yaml"
)
REPORT = _runtime.REPOSITORY_ROOT / "datasets" / "api_samples" / "philosophy_corpus_report.json"


def main(spark_session: Any | None = None) -> str:
    spark_session = _runtime.active_spark(spark_session)
    catalog = _runtime.parameter("catalog", "dev_lakehouse", globals().get("dbutils"))
    return process_job(
        spark_session,
        acon=ACON,
        catalog=catalog,
        variables={"report": str(REPORT)},
    )


if __name__ == "__main__":
    main()
