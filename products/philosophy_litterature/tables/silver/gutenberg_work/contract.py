"""Contract for normalized current Gutenberg catalog works."""
from dataclasses import dataclass
from datetime import date, datetime

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import String


@dataclass
class TableDefinition(BaseSchema):
    gutenberg_id: String
    media_type: String | None
    issued_date: date | None
    title: String
    language_codes: list
    authors: list
    subjects: list
    locc_classes: list
    bookshelves: list
    landing_page_url: String
    source_snapshot_date: date
    source_checksum: String
    source_file: String
    ingested_at: datetime

    class Meta:
        object_name = "gutenberg_work"
        object_location = "silver.gutenberg_work"
        object_description = "Latest normalized official Gutenberg catalog row per Text#."
        column_constraints = {
            "gutenberg_id": {"PK": True},
            "title": {"NOT_BLANK": True},
        }
        custom_table_properties = {"delta.enableChangeDataFeed": "true"}
