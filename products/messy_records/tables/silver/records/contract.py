"""Governed contract for ``silver.records`` — the cleaned messy demo feed.

Mirrors the transformation output exactly: every field of ``CLEAN_RECORD``
(see spark_schema.py) plus the Bronze ingestion timestamp carried for lineage.
``record_id`` and ``title`` are non-nullable because the error-level rules in
quality.yaml drop those rows before the write, and ``record_id`` is unique
because the transformation keeps only the latest row per key.
"""
from dataclasses import dataclass
from datetime import datetime

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Boolean, Double, Int, String


@dataclass
class TableDefinition(BaseSchema):
    record_id: String
    title: String
    creators: list
    summary: String | None
    category: String | None
    labels: list
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
    ingested_at: datetime

    class Meta:
        object_name = "records"
        object_location = "silver.records"
        object_description = (
            "Cleaned, typed and deduplicated messy demo records. One row per "
            "record_id, current state. Heterogeneous raw values are coerced to "
            "real column types by the cleaning UDF."
        )
        column_constraints = {"record_id": {"PK": True}}
        custom_table_properties = {"delta.enableChangeDataFeed": "true"}
        column_comments = {
            "record_id": "Natural key from the source system.",
            "title": "Cleaned title (trimmed, HTML-unescaped).",
            "creators": "Normalized person names (First Last).",
            "summary": "Cleaned free text.",
            "category": "Standardized category label.",
            "labels": "Lowercased, de-duplicated labels.",
            "year": "Publication year parsed from messy input.",
            "rating": "Rating parsed to double (handles European decimal commas).",
            "is_public": "Boolean parsed from yes/no/1/0/true/false.",
            "price": "Price parsed to double (currency symbols stripped).",
            "email": "Validated, lowercased email, null when invalid.",
            "url": "URL with scheme ensured, null when blank.",
            "lat": "Latitude parsed from the geo object.",
            "lon": "Longitude parsed from the geo object.",
            "language": "Normalized language code.",
            "updated_at": "Source update date parsed to an ISO date string.",
            "ingested_at": "Bronze ingestion timestamp retained for lineage.",
        }


TABLE = TableDefinition.Meta.object_location
