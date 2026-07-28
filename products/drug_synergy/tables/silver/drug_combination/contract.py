"""Cleaned, canonicalised drug-combination measurements.

Grain: one row per (drug_min, drug_max, cell_line_key) — the natural key. Because
the pair is canonicalised, A+B and B+A collapse to the same row, and repeated
screens of the same pair are averaged.
"""
from dataclasses import dataclass
from datetime import datetime

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Boolean, Double, Int, String


@dataclass
class TableDefinition(BaseSchema):
    drug_min: String
    drug_max: String
    cell_line_key: String
    cell_line_raw: String | None
    synergy_zip: Double | None
    synergy_bliss: Double | None
    synergy_loewe: Double | None
    synergy_hsa: Double | None
    is_synergistic: Boolean | None
    is_antagonistic: Boolean | None
    n_measurements: Int
    ingested_at: datetime

    class Meta:
        object_name = "drug_combination"
        object_location = "silver.drug_combination"
        object_description = (
            "One row per canonical drug pair and cell line. Scores are the mean "
            "across repeated screens; synergy labels use the conventional +/-10 "
            "cutoff on the ZIP score."
        )
        column_constraints = {
            "drug_min": {"PK": True},
            "drug_max": {"PK": True},
            "cell_line_key": {"PK": True},
        }
        custom_table_properties = {"delta.enableChangeDataFeed": "true"}
        column_comments = {
            "drug_min": "Alphabetically first drug of the canonical pair.",
            "drug_max": "Alphabetically second drug of the canonical pair.",
            "cell_line_key": "Normalised cell-line name; joins to silver.cell_line.",
            "cell_line_raw": "Cell-line string as written by the source.",
            "synergy_zip": "Mean ZIP synergy score across measurements.",
            "is_synergistic": "ZIP > 10 — the conventional synergy cutoff.",
            "is_antagonistic": "ZIP < -10.",
            "n_measurements": "Screens averaged into this row.",
        }


TABLE = TableDefinition.Meta.object_location
