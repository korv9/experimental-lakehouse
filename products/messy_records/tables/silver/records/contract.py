from dataclasses import dataclass
from datetime import datetime

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Bigint, Boolean, Double, Int, String


@dataclass
class TableDefinition(BaseSchema):
    # silver = cleaned, typed, deduplicated. Surrogate + business keys, one row
    # per record, current state. Every messy raw type is now a real column type.
    sk_record: Bigint
    bk_record_id: String
    title: String | None
    creators: list                      # array<string>
    summary: String | None
    category: String | None
    labels: list                        # array<string>
    year: Int | None
    rating: Double | None
    is_public: Boolean | None
    price: Double | None
    email: String | None
    url: String | None
    lat: Double | None
    lon: Double | None
    language: String | None
    updated_at: String | None
    dp_ingestion_ts: datetime | None
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
