"""Governed contract for the raw messy demo landing table.

Bronze keeps every source record verbatim in ``raw_payload`` plus ingestion
metadata, append-only, so Silver can always be rebuilt without calling the
source again. There is no primary key: raw ids may be null or duplicated —
that is exactly the mess this product exists to demonstrate.
"""
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
        object_name = "messy_demo_records"
        object_location = "bronze.messy_demo_records"
        object_description = (
            "Raw landing for the messy demo source. One row per ingested source "
            "record, append-only. Full record preserved verbatim in raw_payload."
        )
        column_constraints = {}  # no PK: raw ids may be null or duplicated
        custom_table_properties = {"delta.appendOnly": "true"}
        column_comments = {
            "raw_payload": "Complete source record preserved as JSON.",
            "source_record_id": "Source id as received; may be null in this feed.",
            "batch_id": "Identifier of the ingestion batch.",
            "ingested_at": "UTC timestamp when the source record landed.",
        }


TABLE = TableDefinition.Meta.object_location
