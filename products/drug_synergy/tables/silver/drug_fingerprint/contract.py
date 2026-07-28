"""Morgan (ECFP4) fingerprints derived from SMILES.

Stored as the *indices of the active bits* rather than a 2048-column table: the
vector is sparse (typically 30-60 bits set), so the array is far smaller and the
table does not need 2048 columns. Expand to a dense vector at model time.
"""
from dataclasses import dataclass
from datetime import datetime

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Int, String


@dataclass
class TableDefinition(BaseSchema):
    drug_name: String
    active_bits: list
    n_active_bits: Int
    n_bits: Int
    radius: Int
    ingested_at: datetime

    class Meta:
        object_name = "drug_fingerprint"
        object_location = "silver.drug_fingerprint"
        object_description = (
            "Morgan/ECFP4 fingerprint per drug, radius 2 over 2048 bits, stored "
            "as active bit indices. Derived from silver.drug.smiles, so it is "
            "reproducible without re-calling PubChem."
        )
        column_constraints = {"drug_name": {"PK": True}}
        custom_table_properties = {"delta.enableChangeDataFeed": "true"}
        column_comments = {
            "active_bits": "Indices of set bits (sparse representation).",
            "n_active_bits": "Number of set bits — a crude structural complexity signal.",
            "n_bits": "Fingerprint length the indices refer to (2048).",
            "radius": "Morgan radius (2 = ECFP4).",
        }


TABLE = TableDefinition.Meta.object_location
