"""DepMap cell lines, keyed so DrugComb can join to them."""
from dataclasses import dataclass
from datetime import datetime

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import String


@dataclass
class TableDefinition(BaseSchema):
    cell_line_key: String
    model_id: String
    cell_line_name: String | None
    stripped_cell_line_name: String | None
    oncotree_lineage: String | None
    depmap_release: String
    ingested_at: datetime

    class Meta:
        object_name = "cell_line"
        object_location = "silver.cell_line"
        object_description = (
            "One row per DepMap cell line, current release. cell_line_key is the "
            "normalised name that DrugComb rows join on; model_id is DepMap's own "
            "identifier and joins to expression."
        )
        column_constraints = {"cell_line_key": {"PK": True}}
        custom_table_properties = {"delta.enableChangeDataFeed": "true"}
        column_comments = {
            "cell_line_key": "Normalised StrippedCellLineName; the cross-source join key.",
            "model_id": "DepMap ModelID.",
            "oncotree_lineage": "Cancer type / tissue lineage — the basis of gold.dim_cancer_type.",
            "depmap_release": "DepMap release the row came from, e.g. 24Q2.",
        }


TABLE = TableDefinition.Meta.object_location
