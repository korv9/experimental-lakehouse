from dataclasses import dataclass

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Bigint, Boolean, Double, Int


@dataclass
class TableDefinition(BaseSchema):
    drug_min_key: Bigint
    drug_max_key: Bigint
    cell_line_key: Bigint
    cancer_type_key: Bigint
    synergy_zip: Double | None
    synergy_bliss: Double | None
    synergy_loewe: Double | None
    synergy_hsa: Double | None
    is_synergistic: Boolean | None
    n_measurements: Int
    combination_count: Bigint

    class Meta:
        object_name = "fact_drug_synergy"
        object_location = "gold.fact_drug_synergy"
        object_description = (
            "Grain: one row per canonical drug pair and cell line. Foreign keys "
            "only, plus additive measures — descriptive attributes stay in the "
            "dimensions so the same measures can be sliced by drug, cell line or "
            "cancer type."
        )
        column_constraints = {}  # composite grain; uniqueness is asserted in tests
        custom_table_properties = {"delta.enableChangeDataFeed": "true"}
        column_comments = {
            "drug_min_key": "FK to gold.dim_drug (first drug of the canonical pair).",
            "drug_max_key": "FK to gold.dim_drug (second drug).",
            "cell_line_key": "FK to gold.dim_cell_line.",
            "cancer_type_key": "FK to gold.dim_cancer_type.",
            "combination_count": "Additive count measure, always 1.",
            "n_measurements": "Screens averaged into this row.",
        }
