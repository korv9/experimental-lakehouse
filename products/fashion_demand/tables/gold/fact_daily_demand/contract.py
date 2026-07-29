"""Governed contract for ``gold.fact_daily_demand`` — the demand grain.

One row per article, sales channel and day. This is the Kimball fact the ML
layer trains on, and its grain *is* the forecasting problem: predict
``units_sold`` for an (article, channel) some days ahead.

Two decisions worth stating because they shape everything downstream:

**Zero-demand days are rows, not gaps.** A day an article sold nothing is a real
observation, and a model that only ever sees days with sales learns that demand
is always positive. The Silver-to-Gold step densifies the panel against
``dim_date`` for the period each article was actually on sale.

**``mean_unit_price`` is an outcome, not an input.** In this source, price is
observed from transactions, so it exists only where a sale happened. It belongs
in the fact for reporting, but the feature layer may only use it *lagged* —
today's realised price is not knowable before today's demand.
"""
from dataclasses import dataclass
from datetime import date, datetime

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Bigint, Double, Int, String


@dataclass
class TableDefinition(BaseSchema):
    article_id: String
    sales_channel_id: Int
    demand_date: date
    units_sold: Bigint
    gross_revenue: Double
    mean_unit_price: Double | None
    customer_count: Bigint
    loaded_at: datetime

    class Meta:
        object_name = "fact_daily_demand"
        object_location = "gold.fact_daily_demand"
        object_description = (
            "Daily units sold per article and sales channel. Densified: days "
            "with no sales are present with units_sold = 0. The training grain "
            "for the demand forecasting models."
        )
        column_constraints = {
            "article_id": {"PK": True},
            "sales_channel_id": {"PK": True},
            "demand_date": {"PK": True},
        }
        custom_table_properties = {"delta.enableChangeDataFeed": "true"}
        column_comments = {
            "article_id": "Foreign key to gold.dim_article.",
            "sales_channel_id": "Foreign key to gold.dim_sales_channel (1 = store, 2 = online).",
            "demand_date": "Foreign key to gold.dim_date. The day demand was observed.",
            "units_sold": "Additive measure: items sold that day. Zero on no-sale days.",
            "gross_revenue": "Additive measure: summed transaction price.",
            "mean_unit_price": (
                "Non-additive: revenue / units. Null on zero-demand days. "
                "Feature layer may only use this lagged."
            ),
            "customer_count": (
                "Semi-additive: distinct customers that day. Do not sum across days."
            ),
            "loaded_at": "When this row was written, for lineage.",
        }
