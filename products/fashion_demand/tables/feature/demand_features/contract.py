"""Governed contract for ``feature.demand_features`` — the model's input shape.

A feature table earns a contract for the same reason a Silver table does, and
then one more: a model is trained against a column list, and if that list drifts
between training and scoring the model does not fail, it just gets quietly
worse. Pinning the schema here means a renamed or retyped feature breaks the
pipeline at the write, before anything is scored with it.

Everything numeric is a double, including the lags of an integer count. Models
consume floats, and a bigint that becomes a double somewhere between training
and inference is a bug that costs an afternoon.
"""
from dataclasses import dataclass
from datetime import date, datetime

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Boolean, Double, Int, String


@dataclass
class TableDefinition(BaseSchema):
    article_id: String
    sales_channel_id: Int
    feature_date: date

    # Demand on the forecast origin itself. Observed by end of feature_date, so
    # using it is honest — and it is the seasonal-naive baseline when the
    # horizon is a whole number of weeks.
    units_sold: Double

    # Recent demand. Null early in a series' life, which is legitimate — the
    # model should learn from a cold start rather than from an invented zero.
    units_sold_lag_1: Double | None
    units_sold_lag_7: Double | None
    units_sold_lag_14: Double | None
    units_sold_lag_28: Double | None
    units_sold_mean_7d: Double | None
    units_sold_stddev_7d: Double | None
    units_sold_mean_28d: Double | None
    units_sold_stddev_28d: Double | None

    # Price, lagged. Never the same-day value: see transform._price.
    mean_unit_price_lag_1: Double | None

    # Calendar. Derived from feature_date alone, so always present.
    day_of_week: Int
    day_of_month: Int
    week_of_year: Int
    month: Int
    year: Int
    is_weekend: Boolean

    # The label, and the horizon it was built for.
    target: Double
    horizon_days: Int
    loaded_at: datetime

    class Meta:
        object_name = "demand_features"
        object_location = "feature.demand_features"
        object_description = (
            "Supervised training rows for demand forecasting. One row per "
            "article, sales channel and feature_date; `target` is units_sold "
            "`horizon_days` after feature_date. Every feature uses only data "
            "observable on or before feature_date."
        )
        column_constraints = {
            "article_id": {"PK": True},
            "sales_channel_id": {"PK": True},
            "feature_date": {"PK": True},
        }
        custom_table_properties = {"delta.enableChangeDataFeed": "true"}
        column_comments = {
            "article_id": "Foreign key to gold.dim_article.",
            "sales_channel_id": "1 = store, 2 = online.",
            "feature_date": "The forecast origin. All features are as of this day.",
            "units_sold": "Units sold on feature_date itself. Observed at the origin.",
            "units_sold_lag_1": "Units sold exactly 1 day earlier.",
            "units_sold_lag_7": "Units sold exactly 7 days earlier (same weekday).",
            "units_sold_lag_14": "Units sold exactly 14 days earlier.",
            "units_sold_lag_28": "Units sold exactly 28 days earlier.",
            "units_sold_mean_7d": "Mean units over the 7 days before feature_date.",
            "units_sold_stddev_7d": "Volatility over the 7 days before feature_date.",
            "units_sold_mean_28d": "Mean units over the 28 days before feature_date.",
            "units_sold_stddev_28d": "Volatility over the 28 days before feature_date.",
            "mean_unit_price_lag_1": (
                "Realised unit price 1 day earlier. The same-day price is a "
                "function of the target and is deliberately absent."
            ),
            "day_of_week": "1 = Sunday, per Spark's dayofweek.",
            "day_of_month": "Day of month, for pay-cycle effects.",
            "week_of_year": "ISO week, so seasons align year over year.",
            "month": "Month of year.",
            "year": "Calendar year.",
            "is_weekend": "True on Saturday and Sunday.",
            "target": "Units sold horizon_days after feature_date. The label.",
            "horizon_days": "How far ahead target looks. Fixed per build.",
            "loaded_at": "When this row was written, for lineage.",
        }
