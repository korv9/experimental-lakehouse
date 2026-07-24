from dataclasses import dataclass

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Bigint, Int, String


@dataclass
class TableDefinition(BaseSchema):
    work_key: Bigint
    work_id: String
    title: String
    language: String | None
    year: Int | None

    class Meta:
        object_name = "dim_work"
        object_location = "gold.dim_work"
        column_constraints = {"work_key": {"PK": True}}
        column_comments = {
            "work_key": "Surrogate key for the Work dimension.",
            "work_id": "Natural key from the source.",
        }
