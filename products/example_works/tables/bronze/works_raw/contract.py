"""Governed contract for the raw Example Works landing table."""
from dataclasses import dataclass
from datetime import datetime

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Int, String


@dataclass
class TableDefinition(BaseSchema):
    source_name: String
    source_endpoint: String
    ingested_at: datetime
    batch_id: String
    request_parameters: String
    http_status: Int
    source_record_id: String | None
    raw_payload: String
    schema_version: String

    class Meta:
        object_name = "example_data_records"
        object_location = "bronze.example_data_records"
        column_constraints = {}
        custom_table_properties = {"delta.appendOnly": "true"}
        column_comments = {
            "raw_payload": "Complete source record preserved as JSON.",
            "batch_id": "Identifier of the ingestion batch.",
            "ingested_at": "UTC timestamp when the source record landed.",
        }


TABLE = TableDefinition.Meta.object_location
