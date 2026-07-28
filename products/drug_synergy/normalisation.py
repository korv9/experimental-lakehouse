"""Identifier normalisation for this domain.

Drug and cell-line names are written differently in every source: DrugComb has
``A-549``, DepMap has ``A549`` in StrippedCellLineName and ``A-549`` in
CellLineName, and the same drug appears with stray casing and whitespace. These
helpers produce the join keys, and they are Spark-native (no UDF) because the
rules are pure string operations.

They mirror the original project's ``BRAclean.py`` and ``Omic_ny.py``: cell
lines collapse to alphanumerics, drugs keep their internal punctuation because
it is chemically meaningful (``5-fluorouracil`` is not ``5 fluorouracil``).
"""
from __future__ import annotations

from pyspark.sql import functions as F


def normalise_drug(column):
    """'  Paclitaxel ' -> 'paclitaxel'. Case and whitespace only."""
    collapsed = F.regexp_replace(F.trim(column), r"\s+", " ")
    return F.nullif(F.lower(collapsed), F.lit(""))


def normalise_cell_line(column):
    """'A-549' / 'HCT 116' -> 'a549' / 'hct116'.

    Strips every non-alphanumeric character, which is what makes DrugComb's
    cell-line strings joinable to DepMap's StrippedCellLineName.
    """
    stripped = F.regexp_replace(F.lower(F.trim(column)), r"[^a-z0-9]", "")
    return F.nullif(stripped, F.lit(""))


def to_double(column):
    """Numeric score or null — 'N/A' and blanks become null rather than 0."""
    cleaned = F.trim(column)
    return F.when(
        cleaned.rlike(r"^-?\d+(\.\d+)?$"), cleaned.cast("double")
    ).otherwise(F.lit(None).cast("double"))
