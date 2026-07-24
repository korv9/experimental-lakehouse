from dataclasses import dataclass
from datetime import date

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Int


@dataclass
class TableDefinition(BaseSchema):
    date_key: Int
    full_date: date
    calendar_year: Int
    calendar_quarter: Int
    calendar_month: Int
    day_of_month: Int

    class Meta:
        object_name = "dim_date"
        object_location = "gold.dim_date"
        column_constraints = {"date_key": {"PK": True}}
        column_comments = {
            "date_key": "Integer date key in yyyyMMdd format.",
            "full_date": "Calendar date.",
        }
