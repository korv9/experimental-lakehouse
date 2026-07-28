"""Drugs resolved to PubChem identifiers and structures."""
from dataclasses import dataclass
from datetime import datetime

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Bigint, String


@dataclass
class TableDefinition(BaseSchema):
    drug_name: String
    pubchem_cid: Bigint | None
    smiles: String | None
    resolved_by: String | None
    ingested_at: datetime

    class Meta:
        object_name = "drug"
        object_location = "silver.drug"
        object_description = (
            "One row per normalised drug name, with its PubChem compound id and "
            "SMILES structure when PubChem could resolve it. Unresolved drugs are "
            "kept with null identifiers so coverage stays measurable."
        )
        column_constraints = {"drug_name": {"PK": True}}
        custom_table_properties = {"delta.enableChangeDataFeed": "true"}
        column_comments = {
            "drug_name": "Normalised drug name; joins to silver.drug_combination.",
            "pubchem_cid": "PubChem Compound ID, null when unresolved.",
            "smiles": "Canonical SMILES; the input to fingerprinting.",
            "resolved_by": "Which lookup succeeded: name or CAS registry number.",
        }


TABLE = TableDefinition.Meta.object_location
