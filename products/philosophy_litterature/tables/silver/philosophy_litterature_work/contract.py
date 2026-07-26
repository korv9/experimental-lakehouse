"""Contract for approved corpus intent joined with official source metadata."""
from dataclasses import dataclass
from datetime import date, datetime

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import String


@dataclass
class TableDefinition(BaseSchema):
    corpus_id: String
    corpus_work_id: String
    gutenberg_id: String
    period: String
    canonical_author: String
    canonical_title: String
    match_status: String
    title: String
    language_codes: list
    authors: list
    subjects: list
    locc_classes: list
    bookshelves: list
    text_url: String | None
    landing_page_url: String
    source_snapshot_date: date
    source_checksum: String
    ingested_at: datetime

    class Meta:
        object_name = "philosophy_litterature_work"
        object_location = "silver.philosophy_litterature_work"
        object_description = (
            "Versioned Philosophy corpus intent joined to normalized official "
            "Project Gutenberg catalog metadata."
        )
        column_constraints = {"corpus_work_id": {"PK": True}}
        custom_table_properties = {"delta.enableChangeDataFeed": "true"}


TABLE = TableDefinition.Meta.object_location
