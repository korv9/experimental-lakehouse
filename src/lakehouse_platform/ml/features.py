"""Point-in-time-correct feature helpers for panel data (entity x date).

Every function here answers the same question: *what was knowable about this
entity on this date, using only earlier rows?* Two details do the real work.

**Windows range over days, not rows.** ``rowsBetween(-6, -1)`` means "the six
previous rows", which equals "the six previous days" only when every entity has
a row for every date. Retail panels have gaps — closed stores, discontinued
articles — so a row-based window silently reaches further back for sparse
entities than for dense ones. These helpers order by an integer day index and
use ``rangeBetween``, so a 7-day window is seven calendar days for everyone.

**Windows end at the previous day.** Every bound here is ``-1``, never ``0``.
A rolling mean that includes today is a rolling mean that includes the target,
and a model trained on it will look extraordinary and forecast nothing.

pyspark is imported inside each function so the module stays importable — and
lintable — without a Spark runtime.
"""
from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

DAY_INDEX = "_day_index"
EPOCH = "1970-01-01"

AGGREGATIONS = ("mean", "sum", "min", "max", "stddev")


def add_day_index(df: DataFrame, *, date_column: str) -> DataFrame:
    """Add the integer day number used to range windows over the calendar."""
    from pyspark.sql import functions as F

    return df.withColumn(
        DAY_INDEX, F.datediff(F.col(date_column), F.lit(EPOCH).cast("date"))
    )


def _window(keys: Sequence[str], *, start: int, end: int):
    # Validate before importing Spark: a bad call is a bad call with or without
    # a cluster, and checking first keeps these guards testable in CI.
    if not keys:
        raise ValueError("a panel window needs at least one entity key")

    from pyspark.sql import Window

    return Window.partitionBy(*keys).orderBy(DAY_INDEX).rangeBetween(start, end)


def add_lags(
    df: DataFrame,
    *,
    value: str,
    keys: Sequence[str],
    date_column: str,
    lags: Sequence[int],
) -> DataFrame:
    """Add ``{value}_lag_{n}`` columns: the value exactly n calendar days ago.

    Missing where the entity has no row on that date, which is correct — the
    model should learn from the absence rather than from an invented zero.
    """
    if any(lag < 1 for lag in lags):
        raise ValueError("lags must be at least 1 day; lag 0 is the target itself")

    from pyspark.sql import functions as F

    indexed = add_day_index(df, date_column=date_column)
    for lag in lags:
        window = _window(keys, start=-lag, end=-lag)
        indexed = indexed.withColumn(f"{value}_lag_{lag}", F.first(F.col(value)).over(window))
    return indexed.drop(DAY_INDEX)


def add_rolling(
    df: DataFrame,
    *,
    value: str,
    keys: Sequence[str],
    date_column: str,
    windows: Sequence[int],
    aggregations: Sequence[str] = ("mean",),
) -> DataFrame:
    """Add ``{value}_{agg}_{n}d`` columns over the n days *before* each row.

    A 7-day window covers days -7 through -1 inclusive. The current day is never
    part of its own feature.
    """
    unknown = [name for name in aggregations if name not in AGGREGATIONS]
    if unknown:
        raise ValueError(f"unsupported aggregations {sorted(unknown)}; use {list(AGGREGATIONS)}")
    if any(days < 1 for days in windows):
        raise ValueError("rolling windows must span at least 1 day")

    from pyspark.sql import functions as F

    indexed = add_day_index(df, date_column=date_column)
    for days in windows:
        window = _window(keys, start=-days, end=-1)
        for name in aggregations:
            function = getattr(F, name)
            indexed = indexed.withColumn(
                f"{value}_{name}_{days}d", function(F.col(value)).over(window)
            )
    return indexed.drop(DAY_INDEX)


def add_calendar(df: DataFrame, *, date_column: str) -> DataFrame:
    """Add the calendar features a demand model always ends up needing.

    Day of week and month carry most of retail seasonality. ``week_of_year`` is
    ISO, so it stays aligned year over year. Nothing here depends on other rows,
    so there is no leakage surface.
    """
    from pyspark.sql import functions as F

    date = F.col(date_column)
    return (
        df.withColumn("day_of_week", F.dayofweek(date))
        .withColumn("day_of_month", F.dayofmonth(date))
        .withColumn("week_of_year", F.weekofyear(date))
        .withColumn("month", F.month(date))
        .withColumn("year", F.year(date))
        .withColumn("is_weekend", F.dayofweek(date).isin(1, 7))
    )


def add_horizon_target(
    df: DataFrame,
    *,
    value: str,
    keys: Sequence[str],
    date_column: str,
    horizon_days: int,
) -> DataFrame:
    """Add ``target`` = the value ``horizon_days`` ahead, and drop rows without one.

    This is the one place a future value is read on purpose: it is the label.
    Keeping it in a named helper means the forward-looking window appears
    exactly once in a product, where it can be reviewed, rather than being
    open-coded next to the features it must never contaminate.
    """
    if horizon_days < 1:
        raise ValueError("horizon_days must be at least 1")

    from pyspark.sql import functions as F

    indexed = add_day_index(df, date_column=date_column)
    window = _window(keys, start=horizon_days, end=horizon_days)
    return (
        indexed.withColumn("target", F.first(F.col(value)).over(window))
        .drop(DAY_INDEX)
        .where(F.col("target").isNotNull())
    )
