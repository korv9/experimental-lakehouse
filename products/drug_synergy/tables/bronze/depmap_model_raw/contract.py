"""Raw DepMap cell-line metadata rows."""
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
        object_name = "depmap_model_raw"
        object_location = "bronze.depmap_model_raw"
        object_description = (
            "Raw DepMap Model.csv rows, one per cell line, append-only. Each "
            "quarterly release lands as a new batch; schema_version carries the "
            "release so Silver can keep the newest."
        )
        column_constraints = {}
        custom_table_properties = {"delta.appendOnly": "true"}
        column_comments = {
            "source_record_id": "DepMap ModelID.",
            "schema_version": "DepMap release, e.g. 24Q2.",
        }


TABLE = TableDefinition.Meta.object_location
