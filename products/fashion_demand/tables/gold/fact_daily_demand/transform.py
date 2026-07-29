"""Silver -> Gold: transaction lines aggregated into a dense daily demand panel.

This module carries more modelling judgement than any other in the product, and
almost all of it is in one step.

**A transactions table only contains days something sold.** Group it by day and
every row you get has positive demand. Train on that and the model never sees a
zero, learns that demand is always positive, and systematically overstocks —
which is the expensive direction of the error. Worse, the panel has holes, so
every lag and rolling window computed over it silently reaches further back for
slow-moving articles than for fast ones.

So the aggregate is *densified*: for each (article, channel), every calendar day
between its first and last observed sale becomes a row, with ``units_sold = 0``
where nothing sold. The window is per series rather than global on purpose — an
article launched in November should not have three hundred fabricated zeros in
front of it, because it was not on sale, and "did not sell" and "could not be
sold" are different facts.

``min_total_units`` exists for laptops. The full panel is roughly 105k articles
across two channels and two years; restricting to articles above a volume
threshold makes it something a local Spark session finishes.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from lakehouse_platform.observability.progress import progress

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

KEYS = ("article_id", "sales_channel_id")


def _aggregate(df: DataFrame) -> DataFrame:
    """One row per article, channel and day that had at least one sale.

    Each Silver row is one purchased item, so ``count`` is the unit count. No
    separate transaction count is kept: in this source it would be the same
    number under a different name.
    """
    from pyspark.sql import functions as F

    return df.groupBy(*KEYS, F.col("transaction_date").alias("demand_date")).agg(
        F.count(F.lit(1)).cast("long").alias("units_sold"),
        F.coalesce(F.sum("price"), F.lit(0.0)).alias("gross_revenue"),
        F.countDistinct("customer_id").cast("long").alias("customer_count"),
    )


def _busy_enough(df: DataFrame, minimum: int) -> DataFrame:
    """Keep only series whose total volume clears ``minimum``."""
    if minimum <= 0:
        return df

    from pyspark.sql import functions as F

    totals = df.groupBy(*KEYS).agg(F.sum("units_sold").alias("_total"))
    kept = totals.where(F.col("_total") >= minimum).select(*KEYS)
    progress("FASHION_DEMAND", "Filtering low-volume series", min_total_units=minimum)
    return df.join(kept, list(KEYS), "inner")


def _densify(df: DataFrame) -> DataFrame:
    """Fill in the days each series existed but sold nothing."""
    from pyspark.sql import functions as F

    bounds = df.groupBy(*KEYS).agg(
        F.min("demand_date").alias("_first"), F.max("demand_date").alias("_last")
    )
    calendar = bounds.select(
        *KEYS,
        F.explode(
            F.sequence(F.col("_first"), F.col("_last"), F.expr("INTERVAL 1 DAY"))
        ).alias("demand_date"),
    )
    dense = calendar.join(df, [*KEYS, "demand_date"], "left")

    # A day with no sales sold nothing and earned nothing. mean_unit_price stays
    # null rather than zero: there was no price, and zero would drag every
    # lagged price feature toward it.
    return (
        dense.withColumn("units_sold", F.coalesce(F.col("units_sold"), F.lit(0)).cast("long"))
        .withColumn("gross_revenue", F.coalesce(F.col("gross_revenue"), F.lit(0.0)))
        .withColumn(
            "customer_count", F.coalesce(F.col("customer_count"), F.lit(0)).cast("long")
        )
    )


def _select(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    return df.select(
        "article_id",
        "sales_channel_id",
        "demand_date",
        "units_sold",
        "gross_revenue",
        F.when(F.col("units_sold") > 0, F.col("gross_revenue") / F.col("units_sold"))
        .cast("double")
        .alias("mean_unit_price"),
        "customer_count",
        F.current_timestamp().alias("loaded_at"),
    )


def transform(df: DataFrame, options: dict | None = None) -> DataFrame:
    """Silver transactions -> the dense daily demand fact.

    Options:
        min_total_units  drop series below this lifetime volume (0 = keep all)
    """
    options = options or {}
    minimum = int(options.get("min_total_units", 0))
    progress("FASHION_DEMAND", "Building daily demand", min_total_units=minimum)
    return _select(_densify(_busy_enough(_aggregate(df), minimum)))
