"""Governed contract for ``gold.dim_article``.

Every descriptive column is non-nullable and defaults to ``"Unknown"``. That is
a dimensional-modelling convention rather than a stylistic one: a null in a
dimension attribute drops the row out of a filtered query, so "how did
outerwear do" silently excludes the articles whose department was never
recorded. An explicit ``Unknown`` member keeps them countable.
"""
from dataclasses import dataclass
from datetime import datetime

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Bigint, String


@dataclass
class TableDefinition(BaseSchema):
    article_key: Bigint
    article_id: String
    product_name: String
    product_type: String
    product_group: String
    colour_group: String
    department: String
    index_group: String
    section: String
    garment_group: String
    loaded_at: datetime

    class Meta:
        object_name = "dim_article"
        object_location = "gold.dim_article"
        object_description = (
            "One current row per article, with the descriptive attributes "
            "demand is sliced by. Type 1: attributes are overwritten in place."
        )
        column_constraints = {
            "article_key": {"PK": True},
            "article_id": {"NOT_BLANK": True},
        }
        custom_table_properties = {"delta.enableChangeDataFeed": "true"}
        column_comments = {
            "article_key": "Deterministic surrogate key hashed from article_id.",
            "article_id": "Natural key from the source.",
            "product_name": "Product name, 'Unknown' when absent.",
            "product_type": "Fine-grained type, e.g. Sweater.",
            "product_group": "Coarser grouping, e.g. Garment Upper body.",
            "colour_group": "Colour family.",
            "department": "Owning department.",
            "index_group": "Top-level division, e.g. Ladieswear.",
            "section": "Merchandising section.",
            "garment_group": "Garment family used for assortment planning.",
            "loaded_at": "When this row was written, for lineage.",
        }
