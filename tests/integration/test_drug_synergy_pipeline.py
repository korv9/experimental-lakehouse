"""End-to-end DataFrame test for the drug synergy cleaning, on local Spark.

Runs the real transformation over the checked-in DrugComb fixture and asserts
the cleaning decisions the original BRAclean.py made: canonical pairs collapse,
repeats average, self-pairs disappear, and bad rows survive for the quality gate
to quarantine rather than vanishing here.

Skipped when pyspark is not installed.
"""
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import Row, SparkSession  # noqa: E402

from products.drug_synergy.tables.silver.drug_combination.transform import (  # noqa: E402
    transform,
)

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "datasets" / "drug_synergy" / "drugcombs_scored_sample.csv"


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("drug-synergy-it").getOrCreate()


@pytest.fixture(scope="module")
def bronze(spark):
    with FIXTURE.open(encoding="utf-8") as handle:
        records = list(csv.DictReader(handle))
    rows = [
        Row(
            raw_payload=json.dumps(record, ensure_ascii=False),
            ingested_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        for record in records
    ]
    return spark.createDataFrame(rows)


@pytest.fixture(scope="module")
def silver(spark, bronze):
    return transform(bronze).cache()


def _row(silver, drug_min, drug_max, cell_line_key):
    matches = [
        row for row in silver.collect()
        if (row["drug_min"], row["drug_max"], row["cell_line_key"])
        == (drug_min, drug_max, cell_line_key)
    ]
    assert len(matches) == 1, f"expected exactly one row for {drug_min}+{drug_max}"
    return matches[0]


def test_reversed_pairs_collapse_and_average(silver):
    """Paclitaxel+Carboplatin appears three times: two orders, two spellings."""
    row = _row(silver, "carboplatin", "paclitaxel", "a549")
    assert row["n_measurements"] == 3
    assert row["synergy_zip"] == pytest.approx((14.2 + 12.6 + 15.0) / 3)


def test_self_pairs_are_dropped(silver):
    """A drug combined with itself is not a combination."""
    assert not [row for row in silver.collect() if row["drug_min"] == row["drug_max"]]


def test_rows_missing_a_drug_survive_for_the_quality_gate(silver):
    """They must not disappear silently — quarantine makes them visible."""
    incomplete = [row for row in silver.collect() if row["drug_max"] is None]
    assert len(incomplete) == 1


def test_cell_line_spellings_unify(silver):
    """HCT-116 and HCT116 are the same cell line."""
    keys = {row["cell_line_key"] for row in silver.collect()}
    assert "hct116" in keys
    assert not {key for key in keys if key and "-" in key}


def test_non_numeric_score_is_null_and_does_not_block_the_row(silver):
    row = _row(silver, "everolimus", "sorafenib", "hepg2")
    assert row["synergy_zip"] is None
    assert row["synergy_bliss"] == pytest.approx(7.2)


def test_synergy_labels_use_the_conventional_cutoff(silver):
    assert _row(silver, "5-fluorouracil", "oxaliplatin", "hct116")["is_synergistic"] is True
    assert _row(silver, "cisplatin", "doxorubicin", "mcf7")["is_antagonistic"] is True
    # inside +/-10 is neither
    weak = _row(silver, "erlotinib", "gemcitabine", "panc1")
    assert weak["is_synergistic"] is False and weak["is_antagonistic"] is False


def test_expected_row_count_after_cleaning(silver):
    """10 raw rows -> 1 self-pair dropped -> 3 duplicates collapse to 1 -> 7."""
    assert silver.count() == 7
