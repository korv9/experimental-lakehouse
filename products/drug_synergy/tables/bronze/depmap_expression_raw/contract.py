"""Raw DepMap expression rows — one JSON payload per cell line.

The source is a wide matrix (~19,000 gene columns). Keeping the whole row as a
JSON payload means Bronze does not need a 19,000-column schema that changes with
every DepMap release; Silver unpivots it to long format instead.
"""
from dataclasses import dataclass
from datetime import datetime

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import String


@dataclass
class TableDefinition(BaseSchema):
    source_name: String
    source_file: String
    ingested_at: datetime
    batch_id: String
    source_record_id: String | None
    raw_payload: String
    schema_version: String

    class Meta:
        object_name = "depmap_expression_raw"
        object_location = "bronze.depmap_expression_raw"
        object_description = (
            "Raw DepMap RNA expression, one JSON row per cell line holding every "
            "gene column, append-only."
        )
        column_constraints = {}
        custom_table_properties = {"delta.appendOnly": "true"}
        column_comments = {
            "source_record_id": "DepMap ModelID.",
            "raw_payload": "All gene columns for the cell line, as JSON.",
        }


TABLE = TableDefinition.Meta.object_location
