from dataclasses import dataclass

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Bigint, String


@dataclass
class TableDefinition(BaseSchema):
    cell_line_key: Bigint
    cell_line_id: String
    model_id: String | None
    cell_line_name: String | None
    oncotree_lineage: String | None

    class Meta:
        object_name = "dim_cell_line"
        object_location = "gold.dim_cell_line"
        object_description = "One row per cell line screened, with its DepMap annotation."
        column_constraints = {"cell_line_key": {"PK": True}}
        column_comments = {
            "cell_line_key": "Surrogate key for the Cell line dimension.",
            "cell_line_id": "Natural key: the normalised cell-line name.",
            "model_id": "DepMap ModelID; null when the cell line is not in DepMap.",
        }
