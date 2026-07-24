from dataclasses import dataclass

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Bigint, String


@dataclass
class TableDefinition(BaseSchema):
    author_key: Bigint
    author_id: String
    author_name: String | None

    class Meta:
        object_name = "dim_author"
        object_location = "gold.dim_author"
        column_constraints = {"author_key": {"PK": True}}
        column_comments = {
            "author_key": "Surrogate key for the Author dimension.",
            "author_id": "Natural key from the source.",
        }
