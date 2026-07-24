from dataclasses import dataclass

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Bigint, String


@dataclass
class TableDefinition(BaseSchema):
    category_key: Bigint
    category_name: String

    class Meta:
        object_name = "dim_category"
        object_location = "gold.dim_category"
        column_constraints = {"category_key": {"PK": True}}
        column_comments = {
            "category_key": "Surrogate key for the Category dimension.",
            "category_name": "Normalized category name.",
        }
