from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from lakehouse_framework.schemas.base_schema import BaseSchema
from lakehouse_framework.schemas.types import *


@dataclass
class TableDefinition(BaseSchema):
    # silver = cleaned, typed, deduplicated. Surrogate + business keys, one row
    # per record, current state. Every messy raw type is now a real column type.
    sk_record: Bigint
    bk_record_id: String
    title: Optional[String]
    creators: list                      # array<string>
    summary: Optional[String]
    category: Optional[String]
    labels: list                        # array<string>
    year: Optional[Int]
    rating: Optional[Double]
    is_public: Optional[Boolean]
    price: Optional[Double]
    email: Optional[String]
    url: Optional[String]
    lat: Optional[Double]
    lon: Optional[Double]
    language: Optional[String]
    updated_at: Optional[String]
    dp_ingestion_ts: Optional[datetime]
    dp_refresh_ts: datetime

    class Meta:
        object_name = "records"
        object_location = "silver.messy.records"
        object_description = (
            "Cleaned, typed and deduplicated messy records. One row per "
            "bk_record_id, current state. SK is the hashed business key."
        )
        column_constraints = {"sk_record": {"PK": True}}
        custom_table_properties = {"delta.enableChangeDataFeed": "true"}
        column_comments = {
            "sk_record": "PK — hash of bk_record_id (dp_fk_hash).",
            "bk_record_id": "BK — source record id, for lineage back to bronze.",
            "title": "Cleaned title (trimmed, HTML-unescaped).",
            "creators": "Normalized person names (First Last).",
            "summary": "Cleaned free text.",
            "category": "Standardized category label.",
            "labels": "Lowercased, de-duplicated labels.",
            "year": "Publication year parsed from messy input.",
            "rating": "Rating parsed to double (handles currency/European formats).",
            "is_public": "Boolean parsed from yes/no/1/0/true/false.",
            "price": "Price parsed to double.",
            "email": "Validated, lowercased email (null if invalid).",
            "url": "URL with scheme ensured (null if blank).",
            "lat": "Latitude parsed from geo object.",
            "lon": "Longitude parsed from geo object.",
            "language": "Normalized language code.",
            "updated_at": "Source update date parsed to ISO (string).",
            "dp_ingestion_ts": "Bronze ingestion time, carried for lineage.",
            "dp_refresh_ts": "When the row was last written by the platform.",
        }
