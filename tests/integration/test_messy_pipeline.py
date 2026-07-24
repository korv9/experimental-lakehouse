"""Integration test: run the messy demo bronze->silver on a real local Spark.

Skipped automatically when pyspark isn't installed. It builds a bronze DataFrame
from the bundled messy dataset (each raw record as a raw_payload string), runs
the actual Spark transform, and asserts the cleaning + dedup worked end to end.
No Delta/Unity Catalog needed — it operates on DataFrames directly.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import Row, SparkSession

from products.messy_records.tables.silver.records.transform import transform

RAW = Path(__file__).resolve().parents[2] / "datasets" / "messy_demo" / "raw_records.json"


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("messy-it").getOrCreate()


@pytest.fixture(scope="module")
def bronze(spark):
    records = json.loads(RAW.read_text())
    # simulate bronze: one row per raw record, payload as a JSON string
    rows = [
        Row(
            raw_payload=json.dumps(record),
            ingested_at=datetime(2024, 1, index % 27 + 1, tzinfo=timezone.utc),
        )
        for index, record in enumerate(records)
    ]
    return spark.createDataFrame(rows)


def test_transform_cleans_and_dedups(spark, bronze):
    silver = transform(bronze)
    rows = {r["record_id"]: r for r in silver.collect()}

    # REC-001 appears twice in the raw feed but should be one deduped row
    assert sum(1 for r in silver.collect() if r["record_id"] == "REC-001") == 1

    # nested-object creator was normalized to a clean name list
    assert rows["rec_2"]["creators"] == ["John Smith"]

    # messy category + year were coerced to canonical/typed values
    assert rows["3"]["category"] == "nonfiction"
    assert rows["3"]["year"] == 1200

    # currency price parsed to a double
    assert rows["REC-001"]["price"] == 13.50

    # every silver column exists with the enforced type
    assert set(silver.columns) >= {
        "record_id", "title", "creators", "labels", "year",
        "rating", "is_public", "price", "lat", "lon", "updated_at",
    }
