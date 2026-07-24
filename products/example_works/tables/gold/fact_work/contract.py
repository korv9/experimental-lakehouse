from dataclasses import dataclass

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Bigint, Int


@dataclass
class TableDefinition(BaseSchema):
    work_key: Bigint
    author_key: Bigint
    category_key: Bigint
    date_key: Int
    work_count: Bigint
    tag_count: Int

    class Meta:
        object_name = "fact_work"
        object_location = "gold.fact_work"
        column_constraints = {}
        grain = "One row per current work"
        foreign_keys = {
            "work_key": "gold.dim_work.work_key",
            "author_key": "gold.dim_author.author_key",
            "category_key": "gold.dim_category.category_key",
            "date_key": "gold.dim_date.date_key",
        }
        column_comments = {
            "work_count": "Additive count measure; always one per fact row.",
            "tag_count": "Number of source tags assigned to the work.",
        }
