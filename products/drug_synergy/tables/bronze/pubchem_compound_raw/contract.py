"""Raw PubChem responses — one row per resolved drug name.

Landed by the platform REST client rather than a file drop, so this carries the
HTTP fields the file-based sources do not have.
"""
from dataclasses import dataclass
from datetime import datetime

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Int, String


@dataclass
class TableDefinition(BaseSchema):
    # column order and names follow what ingestion.runner writes
    ingestion_id: String
    source_name: String
    source_endpoint: String
    ingested_at: datetime
    batch_id: String
    run_id: String
    request_parameters: String
    http_status: Int
    source_record_id: String | None
    raw_payload: String
    schema_version: String

    class Meta:
        object_name = "pubchem_compound_raw"
        object_location = "bronze.pubchem_compound_raw"
        object_description = (
            "Raw PubChem PUG REST responses for drug-name lookups (CID and "
            "structure), one row per request, append-only. Replaces the original "
            "project's local JSON cache: Bronze is the cache, and it is queryable."
        )
        column_constraints = {"ingestion_id": {"PK": True}}
        custom_table_properties = {"delta.appendOnly": "true"}
        column_comments = {
            "ingestion_id": "Stable hash of source, value and payload; makes replay idempotent.",
            "source_record_id": "The drug name that was looked up.",
            "request_parameters": "Endpoint path and query used, for reproducibility.",
            "http_status": "HTTP status; non-200 rows record failed lookups.",
        }


TABLE = TableDefinition.Meta.object_location
