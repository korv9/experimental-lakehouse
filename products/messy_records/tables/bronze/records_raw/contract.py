"""Governed contract for the raw messy demo landing table.

Bronze keeps every source record verbatim in ``raw_payload`` plus the metadata
needed to trace it back to the file and batch it came from, append-only, so
Silver can always be rebuilt without re-reading the source. There is no primary
key: raw ids may be null or duplicated — that is exactly the mess this product
exists to demonstrate.
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
        object_name = "messy_demo_records"
        object_location = "bronze.messy_demo_records"
        object_description = (
            "Raw landing for the messy demo source. One row per source record, "
            "append-only, preserved verbatim in raw_payload."
        )
        column_constraints = {}  # no PK: raw ids may be null or duplicated
        custom_table_properties = {"delta.appendOnly": "true"}
        column_comments = {
            "source_name": "Source system identifier.",
            "source_file": "File the record was landed from.",
            "raw_payload": "Complete source record preserved as JSON.",
            "source_record_id": "Source id as received; null when the feed omits it.",
            "batch_id": "Identifier of the landing batch.",
            "ingested_at": "UTC timestamp when the source record landed.",
            "schema_version": "Detected/assigned source schema version.",
        }


TABLE = TableDefinition.Meta.object_location
