from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from lakehouse_framework.schemas.base_schema import BaseSchema
from lakehouse_framework.schemas.types import *


@dataclass
class TableDefinition(BaseSchema):
    # bronze = raw source payload + ingestion metadata. No surrogate keys here;
    # ids can be null or duplicated in raw data, so there is no PK.
    bk_record_id: Optional[String]
    raw_payload: String
    source_name: String
    source_endpoint: Optional[String]
    batch_id: String
    schema_version: Optional[String]
    dp_ingestion_ts: datetime
    dp_refresh_ts: datetime

    class Meta:
        object_name = "records"
        object_location = "bronze.messy.records"
        object_description = (
            "Raw landing for the messy demo source. One row per ingested source "
            "record, append-only. Full record preserved verbatim in raw_payload; "
            "silver parses and types it. No surrogate keys in bronze."
        )
        column_constraints = {}  # no PK: raw ids may be null/duplicated
        custom_table_properties = {"delta.appendOnly": "true"}
        column_comments = {
            "bk_record_id": "BK — source record id, extracted from raw_payload (may be null).",
            "raw_payload": "Full source record as received (JSON), unparsed.",
            "source_name": "Source system identifier.",
            "source_endpoint": "API endpoint / path the record came from.",
            "batch_id": "Ingestion batch identifier.",
            "schema_version": "Detected/assigned source schema version.",
            "dp_ingestion_ts": "When the row was first landed in bronze.",
            "dp_refresh_ts": "When the row was last written by the platform.",
        }
