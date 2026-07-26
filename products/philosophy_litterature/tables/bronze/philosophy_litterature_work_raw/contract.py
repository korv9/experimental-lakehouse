"""Contract for raw Gutendex metadata selected by the Philosophy corpus."""
from dataclasses import dataclass
from datetime import datetime

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Int, String


@dataclass
class TableDefinition(BaseSchema):
    ingestion_id: String
    source_name: String
    source_endpoint: String
    ingested_at: datetime
    batch_id: String
    run_id: String
    request_parameters: String
    http_status: Int
    source_record_id: String
    raw_payload: String
    schema_version: String

    class Meta:
        object_name = "philosophy_litterature_work_raw"
        object_location = "bronze.philosophy_litterature_work_raw"
        object_description = (
            "Immutable Gutendex metadata payloads selected by the versioned "
            "Philosophy Books corpus. Business parsing belongs in Silver."
        )
        column_constraints = {"ingestion_id": {"PK": True}}
        custom_table_properties = {"delta.appendOnly": "true"}
        column_comments = {
            "ingestion_id": "Stable hash of source, Gutenberg ID and raw payload.",
            "source_record_id": "Project Gutenberg ebook ID returned by Gutendex.",
            "raw_payload": "Complete Gutendex book object preserved as sorted JSON.",
            "request_parameters": "Exact Gutendex ID-batch request parameters.",
            "run_id": "Foreign-key candidate for platform.pipeline_runs.",
        }


TABLE = TableDefinition.Meta.object_location
