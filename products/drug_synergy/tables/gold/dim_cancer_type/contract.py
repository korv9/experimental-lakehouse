from dataclasses import dataclass

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Bigint, Int, String


@dataclass
class TableDefinition(BaseSchema):
    cancer_type_key: Bigint
    oncotree_lineage: String
    n_cell_lines: Int

    class Meta:
        object_name = "dim_cancer_type"
        object_location = "gold.dim_cancer_type"
        object_description = (
            "One row per Oncotree lineage. Lets the fact be sliced by tissue, "
            "which is the axis most synergy questions are asked along."
        )
        column_constraints = {"cancer_type_key": {"PK": True}}
        column_comments = {
            "cancer_type_key": "Surrogate key for the Cancer type dimension.",
            "oncotree_lineage": "DepMap Oncotree lineage, e.g. Lung, Breast.",
            "n_cell_lines": "Cell lines in this lineage — the denominator for coverage.",
        }
