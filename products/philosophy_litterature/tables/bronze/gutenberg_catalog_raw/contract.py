"""Contract for source-faithful Project Gutenberg catalog rows."""
from dataclasses import dataclass
from datetime import date, datetime

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import String


@dataclass
class TableDefinition(BaseSchema):
    ingestion_id: String
    source_name: String
    source_url: String
    source_file: String
    source_checksum: String
    source_modified_at: String | None
    source_snapshot_date: date
    ingested_at: datetime
    run_id: String
    source_record_id: String
    raw_payload: String
    schema_version: String

    class Meta:
        object_name = "gutenberg_catalog_raw"
        object_location = "bronze.gutenberg_catalog_raw"
        object_description = (
            "Append-only, source-faithful rows from a checksummed official "
            "Project Gutenberg catalog snapshot."
        )
        column_constraints = {"ingestion_id": {"PK": True}}
        custom_table_properties = {"delta.appendOnly": "true"}
        column_comments = {
            "ingestion_id": (
                "Stable hash of source, snapshot checksum, Gutenberg ID and raw row JSON."
            ),
            "source_file": "Governed Unity Catalog Volume path of the source snapshot.",
            "source_checksum": "SHA-256 of the complete compressed catalog file.",
            "source_record_id": "Project Gutenberg Text# value.",
            "raw_payload": "Complete catalog row serialized with original CSV field names.",
        }


TABLE = TableDefinition.Meta.object_location
