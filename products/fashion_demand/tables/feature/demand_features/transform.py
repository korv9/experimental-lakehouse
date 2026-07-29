"""Gold -> feature: turn the demand fact into a supervised learning table.

This is the whole ML feature layer, and it is deliberately an ordinary ACON
transformation: one function, one DataFrame in, one DataFrame out. It gets the
same contract check and the same quality gate as any Silver table, which is the
point — a feature table that silently changes shape breaks a model exactly the
way a Silver table that silently changes shape breaks a dashboard.

Reading order:

``_history``   lags and rolling windows over the *previous* days only
``_price``     the one input that has to be lagged to stay honest
``_calendar``  seasonality, derived from the date alone
``_label``     the future value being predicted, read once, on purpose

The lag and window helpers live in ``lakehouse_platform.ml.features`` because
getting them wrong is the standard way to build a model that scores brilliantly
and forecasts nothing. See that module for why the windows range over days
rather than rows.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from lakehouse_platform.ml.features import (
    add_calendar,
    add_horizon_target,
    add_lags,
    add_rolling,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# The panel identity: one series per article per channel.
KEYS = ("article_id", "sales_channel_id")
DATE_COLUMN = "demand_date"
VALUE = "units_sold"

# Yesterday, last week, a fortnight, four weeks: enough to carry weekday
# seasonality and recent level without exploding the column count.
LAGS = (1, 7, 14, 28)

# 28 days is the widest window, so the training embargo must be at least 28 days
# if you want the strict guarantee. See ml/dataset.py, which asserts it.
ROLLING_WINDOWS = (7, 28)
MAX_WINDOW_DAYS = max(ROLLING_WINDOWS)

HORIZON_DAYS = 14


def _history(df: DataFrame) -> DataFrame:
    """Recent demand — the strongest signal a demand model has."""
    with_lags = add_lags(df, value=VALUE, keys=KEYS, date_column=DATE_COLUMN, lags=LAGS)
    return add_rolling(
        with_lags,
        value=VALUE,
        keys=KEYS,
        date_column=DATE_COLUMN,
        windows=ROLLING_WINDOWS,
        aggregations=("mean", "stddev"),
    )


def _price(df: DataFrame) -> DataFrame:
    """Lag the realised price by one day, then drop the same-day column.

    ``mean_unit_price`` is computed from the day's transactions, so on the day
    being forecast it is not merely unknown — it is a direct function of the
    target. Keeping the lagged value gives the model the pricing signal it
    should have; dropping the raw column means nobody can reintroduce the leak
    by adding it to a feature list later.
    """
    from pyspark.sql import functions as F

    lagged = add_lags(
        df.withColumn("mean_unit_price", F.col("mean_unit_price").cast("double")),
        value="mean_unit_price",
        keys=KEYS,
        date_column=DATE_COLUMN,
        lags=(1,),
    )
    return lagged.drop("mean_unit_price")


def _calendar(df: DataFrame) -> DataFrame:
    """Weekday and season. No other row is involved, so nothing can leak."""
    return add_calendar(df, date_column=DATE_COLUMN)


def _label(df: DataFrame) -> DataFrame:
    """Attach the value HORIZON_DAYS ahead and drop rows that have no future."""
    return add_horizon_target(
        df,
        value=VALUE,
        keys=KEYS,
        date_column=DATE_COLUMN,
        horizon_days=HORIZON_DAYS,
    )


def _select(df: DataFrame) -> DataFrame:
    """Cast to the contract's types and fix the column order."""
    from pyspark.sql import functions as F

    # units_sold at feature_date is kept: the forecast origin is the end of that
    # day, so it is observed, and it is the strongest single feature there is.
    # It is also the seasonal-naive baseline whenever the horizon is a multiple
    # of 7. Dropping it would handicap the model against its own baseline.
    numeric = [VALUE]
    numeric += [f"{VALUE}_lag_{lag}" for lag in LAGS]
    numeric += [f"{VALUE}_{agg}_{days}d" for days in ROLLING_WINDOWS for agg in ("mean", "stddev")]
    numeric += ["mean_unit_price_lag_1"]

    cast = df
    for column in numeric:
        cast = cast.withColumn(column, F.col(column).cast("double"))

    return cast.select(
        "article_id",
        "sales_channel_id",
        F.col(DATE_COLUMN).alias("feature_date"),
        *numeric,
        "day_of_week",
        "day_of_month",
        "week_of_year",
        "month",
        "year",
        "is_weekend",
        F.col("target").cast("double").alias("target"),
        F.lit(HORIZON_DAYS).cast("int").alias("horizon_days"),
        F.current_timestamp().alias("loaded_at"),
    )


def transform(df: DataFrame, options: dict | None = None) -> DataFrame:
    """Gold demand fact -> one supervised row per article, channel and day."""
    return _select(_label(_calendar(_price(_history(df)))))
