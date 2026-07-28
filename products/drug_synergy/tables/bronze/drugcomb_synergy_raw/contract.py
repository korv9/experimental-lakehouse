"""Raw DrugComb screening rows, exactly as downloaded."""
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
        object_name = "drugcomb_synergy_raw"
        object_location = "bronze.drugcomb_synergy_raw"
        object_description = (
            "Raw DrugComb combination-screening rows, one per source row, "
            "append-only. Silver does all cleaning, so a re-download can be "
            "reprocessed without touching the portal again."
        )
        column_constraints = {}  # no PK: the export repeats pairs across studies
        custom_table_properties = {"delta.appendOnly": "true"}
        column_comments = {
            "raw_payload": "Complete source row as JSON.",
            "source_record_id": "drug1|drug2|cell_line as received, before canonicalisation.",
            "batch_id": "Identifier of the download batch.",
        }


TABLE = TableDefinition.Meta.object_location
