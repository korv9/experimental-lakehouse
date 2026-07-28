from dataclasses import dataclass

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Bigint, Int, String


@dataclass
class TableDefinition(BaseSchema):
    drug_key: Bigint
    drug_id: String
    pubchem_cid: Bigint | None
    smiles: String | None
    n_active_bits: Int | None

    class Meta:
        object_name = "dim_drug"
        object_location = "gold.dim_drug"
        object_description = "One row per drug, with structure and fingerprint density."
        column_constraints = {"drug_key": {"PK": True}}
        column_comments = {
            "drug_key": "Surrogate key for the Drug dimension.",
            "drug_id": "Natural key: the normalised drug name.",
            "n_active_bits": "Set bits in the Morgan fingerprint; null when unresolved.",
        }
