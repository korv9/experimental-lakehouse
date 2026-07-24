"""Unit test for the bronze->silver dedup logic.

Transforms are kept as small, testable functions (independent from notebook
state). This test spins up a local Spark session; it's skipped automatically if
pyspark isn't installed.
"""
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from products.example_works.tables.silver.works.transform import _dedupe_latest


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("tests").getOrCreate()


def test_dedupe_keeps_latest_per_key(spark):
    df = spark.createDataFrame(
        [
            ("w1", "old", datetime(2024, 1, 1, tzinfo=timezone.utc)),
            ("w1", "new", datetime(2024, 2, 1, tzinfo=timezone.utc)),
            ("w2", "only", datetime(2024, 1, 1, tzinfo=timezone.utc)),
        ],
        ["work_id", "title", "ingested_at"],
    )
    out = {r["work_id"]: r["title"] for r in _dedupe_latest(df, "work_id").collect()}
    assert out == {"w1": "new", "w2": "only"}
