"""Normalisation and fingerprint logic (Spark only where unavoidable).

These are the rules that decide whether DrugComb and DepMap join at all, so they
are worth pinning down precisely.
"""
import pytest

from products.drug_synergy.fingerprints import N_BITS, RADIUS, morgan_bits


def test_fingerprint_settings_are_ecfp4():
    assert (N_BITS, RADIUS) == (2048, 2)


def test_morgan_bits_returns_empty_without_a_structure():
    """No SMILES is not an error — it is a drug PubChem could not resolve."""
    assert morgan_bits(None) == []
    assert morgan_bits("") == []


# --- Spark-backed normalisation ---------------------------------------------
pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from products.drug_synergy.normalisation import (  # noqa: E402
    normalise_cell_line,
    normalise_drug,
    to_double,
)


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("drug-synergy").getOrCreate()


def _values(spark, rows, column):
    frame = spark.createDataFrame([(value,) for value in rows], ["raw"])
    return [row[0] for row in frame.select(column(frame["raw"])).collect()]


def test_cell_line_normalisation_bridges_drugcomb_and_depmap(spark):
    # DrugComb writes 'A-549', DepMap's StrippedCellLineName is 'A549'
    assert _values(spark, ["A-549", "a549", "HCT 116", "HCT-116", "MCF-7"],
                   normalise_cell_line) == ["a549", "a549", "hct116", "hct116", "mcf7"]


def test_drug_normalisation_keeps_chemically_meaningful_punctuation(spark):
    assert _values(spark, ["  Paclitaxel ", "paclitaxel", "5-Fluorouracil"],
                   normalise_drug) == ["paclitaxel", "paclitaxel", "5-fluorouracil"]


def test_blank_names_become_null_rather_than_empty_strings(spark):
    assert _values(spark, ["   ", ""], normalise_drug) == [None, None]
    assert _values(spark, ["---"], normalise_cell_line) == [None]


def test_non_numeric_scores_become_null_not_zero(spark):
    """'N/A' read as 0.0 would look like 'no synergy' instead of 'no measurement'."""
    assert _values(spark, ["14.2", "-12.4", "N/A", ""], to_double) == [14.2, -12.4, None, None]
