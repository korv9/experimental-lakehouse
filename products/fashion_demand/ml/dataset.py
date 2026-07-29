"""Feature table -> train/test frames, split on time and checked for leakage.

The boundary between the lakehouse and the model lives here, and it is a real
boundary: everything before it is distributed Spark over the whole panel,
everything after it is a single-node pandas frame that LightGBM or scikit-learn
can consume. That is also how it works at scale — features are built where the
data is, training happens on one big node — so the shape does not change when
the data does.

Two guards are deliberate:

* the split is filtered in Spark, so an accidental full ``toPandas()`` of a
  31-million-row table cannot happen by omission;
* ``assert_embargo`` runs before anything is materialised, so a leaking split
  fails in a second rather than after a training run.
"""
from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING

from lakehouse_platform.ml.splits import Split, assert_embargo, rolling_origin_splits, time_split

from products.fashion_demand.tables.feature.demand_features.transform import (
    HORIZON_DAYS,
    MAX_WINDOW_DAYS,
)

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import DataFrame, SparkSession

FEATURE_TABLE = "feature.demand_features"
DATE_COLUMN = "feature_date"
TARGET = "target"

# Columns that identify or annotate a row rather than describe it. Excluded from
# the model matrix by name, so adding a feature to the contract adds it to the
# model automatically and adding a label does not.
NON_FEATURES = ("article_id", "sales_channel_id", "feature_date", "target",
                "horizon_days", "loaded_at")


def feature_columns(columns: list[str]) -> list[str]:
    """The model matrix: everything that is not an identifier or the label."""
    return [name for name in columns if name not in NON_FEATURES]


def read_features(spark: SparkSession, *, catalog: str) -> DataFrame:
    """Read the governed feature table. One place, so nothing reads a stale copy."""
    return spark.table(f"{catalog}.{FEATURE_TABLE}")


def date_range(features: DataFrame) -> tuple[dt.date, dt.date]:
    """The history actually present, so splits are built from data not guesses."""
    from pyspark.sql import functions as F

    row = features.agg(
        F.min(DATE_COLUMN).alias("start"), F.max(DATE_COLUMN).alias("end")
    ).collect()[0]
    if row["start"] is None:
        raise ValueError(f"{FEATURE_TABLE} is empty")
    return row["start"], row["end"]


def build_splits(
    features: DataFrame,
    *,
    test_days: int = 28,
    folds: int = 1,
    strict: bool = True,
) -> list[Split]:
    """Chronological folds over whatever history the feature table holds.

    ``strict`` widens the required embargo from the forecast horizon to the
    widest rolling window, so early test rows share no window with training
    labels. It costs training days; it buys a number you can defend.
    """
    start, end = date_range(features)
    required = max(HORIZON_DAYS, MAX_WINDOW_DAYS) if strict else HORIZON_DAYS

    if folds == 1:
        splits = [
            time_split(
                start, end, test_days=test_days,
                horizon_days=HORIZON_DAYS, embargo_days=required,
            )
        ]
    else:
        splits = rolling_origin_splits(
            start, end, test_days=test_days, horizon_days=HORIZON_DAYS,
            folds=folds, embargo_days=required,
        )

    assert_embargo(splits, minimum_days=required)
    return splits


def materialise(features: DataFrame, split: Split) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Filter to one fold in Spark, then collect train and test to pandas.

    The filter runs before the collect on purpose. Pulling the whole table down
    and slicing it in pandas works on a sample and dies on the real thing.
    """
    from pyspark.sql import functions as F

    date = F.col(DATE_COLUMN)
    train = features.where(date.between(split.train_start, split.train_end))
    test = features.where(date.between(split.test_start, split.test_end))
    return train.toPandas(), test.toPandas()
