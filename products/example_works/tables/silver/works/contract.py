"""Contract and raw source parsing schema for ``silver.works``."""
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Int, String

RAW_WORK = StructType([
    StructField("id", StringType()),
    StructField("title", StringType()),
    StructField("author", StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
    ])),
    StructField("category", StringType()),
    StructField("year", IntegerType()),
    StructField("language", StringType()),
    StructField("tags", ArrayType(StringType())),
    StructField("updated_at", StringType()),
])


@dataclass
class TableDefinition(BaseSchema):
    work_id: String
    title: String
    category: String | None
    year: Int | None
    language: String | None
    tags: list
    author_id: String | None
    author_name: String | None
    updated_at: datetime | None
    ingested_at: datetime

    class Meta:
        object_name = "works"
        object_location = "silver.works"
        column_constraints = {"work_id": {"PK": True}}
        custom_table_properties = {"delta.enableChangeDataFeed": "true"}
        column_comments = {
            "work_id": "Natural key from the source system.",
            "updated_at": "Source last-modified timestamp.",
            "ingested_at": "Bronze ingestion timestamp retained for lineage.",
        }


TABLE = TableDefinition.Meta.object_location
BUSINESS_KEY = "work_id"
