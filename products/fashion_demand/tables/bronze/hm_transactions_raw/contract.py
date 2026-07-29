"""Governed contract for ``bronze.hm_transactions_raw``.

Bronze keeps the source row verbatim in ``raw_payload`` and adds only lineage.
Nothing is typed and nothing is rejected here: if H&M renamed a column tomorrow,
this table should still land the data so the change is visible and diagnosable,
rather than failing an ingestion at 03:00 with no record of what arrived.

``source_record_id`` is a natural key built from the payload fields that
identify a transaction line. The source has no transaction id, so it is not
unique — a customer buying the same article twice on one day is two legitimate
rows. Silver assigns the deterministic surrogate key.
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
        object_name = "hm_transactions_raw"
        object_location = "bronze.hm_transactions_raw"
        object_description = (
            "Raw H&M transaction lines exactly as exported, one JSON payload "
            "per CSV row, plus ingestion lineage. Append-only."
        )
        column_constraints = {}
        custom_table_properties = {"delta.enableChangeDataFeed": "true"}
        column_comments = {
            "source_name": "Logical source, e.g. hm_transactions.",
            "source_file": "File the batch was read from.",
            "ingested_at": "When the row landed.",
            "batch_id": "Groups every row landed by one run.",
            "source_record_id": "t_dat|customer_id|article_id from the payload. Not unique.",
            "raw_payload": "The source row as JSON, every field a string.",
            "schema_version": "Source release marker, e.g. 2022-kaggle.",
        }
