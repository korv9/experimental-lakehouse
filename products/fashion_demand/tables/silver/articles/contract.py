"""Governed contract for ``silver.articles`` — the article master, typed.

Descriptive attributes are nullable because they genuinely are in the source;
only the key is required. A missing colour group is a gap to segment on, not a
row to throw away.
"""
from dataclasses import dataclass
from datetime import datetime

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import String


@dataclass
class TableDefinition(BaseSchema):
    article_id: String
    product_name: String | None
    product_type: String | None
    product_group: String | None
    colour_group: String | None
    department: String | None
    index_group: String | None
    section: String | None
    garment_group: String | None
    ingested_at: datetime

    class Meta:
        object_name = "articles"
        object_location = "silver.articles"
        object_description = (
            "Descriptive attributes per article, deduplicated to one current "
            "row. The subset of the ~25 source columns that demand modelling "
            "and its segmentation actually use."
        )
        column_constraints = {
            "article_id": {"PK": True, "NOT_BLANK": True},
        }
        custom_table_properties = {"delta.enableChangeDataFeed": "true"}
        column_comments = {
            "article_id": "Natural key. String, not numeric.",
            "product_name": "Source prod_name.",
            "product_type": "Fine-grained type, e.g. Sweater. Source product_type_name.",
            "product_group": "Coarser grouping, e.g. Garment Upper body.",
            "colour_group": "Colour family, e.g. Dark Blue.",
            "department": "Owning department.",
            "index_group": "Top-level division, e.g. Ladieswear.",
            "section": "Merchandising section.",
            "garment_group": "Garment family used for assortment planning.",
            "ingested_at": "When the underlying Bronze row landed.",
        }
