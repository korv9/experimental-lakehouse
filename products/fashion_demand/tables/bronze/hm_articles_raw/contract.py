"""Governed contract for ``bronze.hm_articles_raw``.

The article master: ~105k products with around 25 descriptive columns —
product type, colour, department, garment group, section. Landed the same way as
transactions, untyped, so a source that adds an attribute lands rather than
fails.

Only a handful of these columns reach Gold. The rest stay here, queryable, for
whoever later wants to know whether the model does worse on outerwear.
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
        object_name = "hm_articles_raw"
        object_location = "bronze.hm_articles_raw"
        object_description = (
            "Raw H&M article master exactly as exported, one JSON payload per "
            "CSV row, plus ingestion lineage. Append-only."
        )
        column_constraints = {}
        custom_table_properties = {"delta.enableChangeDataFeed": "true"}
        column_comments = {
            "source_name": "Logical source, e.g. hm_articles.",
            "source_file": "File the batch was read from.",
            "ingested_at": "When the row landed.",
            "batch_id": "Groups every row landed by one run.",
            "source_record_id": "article_id from the payload.",
            "raw_payload": "The source row as JSON, every field a string.",
            "schema_version": "Source release marker, e.g. 2022-kaggle.",
        }
