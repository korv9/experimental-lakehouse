"""Time-ordered train/test splits — pure Python, tested without Spark.

A random ``train_test_split`` on a time series is the single most common way to
report an accuracy that does not exist. Cutting on date fixes the obvious half:
the model no longer trains on rows dated after the rows it is scored on.

The half that survives a chronological cut is the *horizon*. Forecasting 14 days
ahead means a training example dated ``d`` carries a label observed at ``d`` but
features known only as of ``d - 14``. Train right up to the day before the test
window and the last 14 training labels describe days the model is supposed to be
predicting blind. The fix is an **embargo**: a gap of unused days between
training and test, at least as wide as the forecast horizon.

A second, milder reason for the same gap: rolling features for early test days
are computed over the final training days, so a model tuned on those labels is
validated slightly optimistically. Widening the embargo to the longest feature
window removes that too, at the cost of training data.

This module makes the embargo an explicit, defaulted parameter rather than
something a notebook silently omits.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass


@dataclass(frozen=True)
class Split:
    """One fold: an inclusive training window, a gap, an inclusive test window."""

    name: str
    train_start: dt.date
    train_end: dt.date
    test_start: dt.date
    test_end: dt.date

    def __post_init__(self) -> None:
        if self.train_start > self.train_end:
            raise ValueError(f"{self.name}: training window is empty")
        if self.test_start > self.test_end:
            raise ValueError(f"{self.name}: test window is empty")
        if self.test_start <= self.train_end:
            raise ValueError(f"{self.name}: test window overlaps training window")

    @property
    def embargo_days(self) -> int:
        """Unused days between training and test. Zero means no gap."""
        return (self.test_start - self.train_end).days - 1

    @property
    def train_days(self) -> int:
        return (self.train_end - self.train_start).days + 1

    @property
    def test_days(self) -> int:
        return (self.test_end - self.test_start).days + 1

    def contains_train(self, day: dt.date) -> bool:
        return self.train_start <= day <= self.train_end

    def contains_test(self, day: dt.date) -> bool:
        return self.test_start <= day <= self.test_end

    def describe(self) -> str:
        return (
            f"{self.name}: train {self.train_start}..{self.train_end} ({self.train_days}d)"
            f" | embargo {self.embargo_days}d"
            f" | test {self.test_start}..{self.test_end} ({self.test_days}d)"
        )


def time_split(
    start: dt.date,
    end: dt.date,
    *,
    test_days: int,
    horizon_days: int,
    embargo_days: int | None = None,
    name: str = "holdout",
) -> Split:
    """One chronological split with the test window at the end of the history.

    ``embargo_days`` defaults to ``horizon_days``: forecasting 14 days out means
    the last 14 training labels describe days inside the horizon being predicted,
    so they are dropped. Pass ``0`` deliberately for a one-step nowcast where the
    forecast origin is the day before each label.
    """
    if test_days < 1:
        raise ValueError("test_days must be at least 1")
    if horizon_days < 1:
        raise ValueError("horizon_days must be at least 1")
    gap = horizon_days if embargo_days is None else embargo_days
    if gap < 0:
        raise ValueError("embargo_days cannot be negative")

    test_end = end
    test_start = test_end - dt.timedelta(days=test_days - 1)
    train_end = test_start - dt.timedelta(days=gap + 1)
    if train_end < start:
        raise ValueError(
            f"history {start}..{end} is too short for test_days={test_days} "
            f"and an embargo of {gap} days"
        )
    return Split(
        name=name,
        train_start=start,
        train_end=train_end,
        test_start=test_start,
        test_end=test_end,
    )


def rolling_origin_splits(
    start: dt.date,
    end: dt.date,
    *,
    test_days: int,
    horizon_days: int,
    folds: int,
    embargo_days: int | None = None,
    step_days: int | None = None,
    expanding: bool = True,
) -> list[Split]:
    """Backtest folds that walk the forecast origin forward through history.

    This is how a forecast is actually validated: retrain as of several past
    dates and score each against what happened next. A single holdout measures
    one week's weather; several folds measure the model.

    ``expanding=True`` grows the training window each fold (the production
    behaviour — you never throw away history). ``expanding=False`` slides a
    fixed-length window instead, which is the right choice when older seasons
    stop being representative.

    Folds are returned oldest-first.
    """
    if folds < 1:
        raise ValueError("folds must be at least 1")
    step = test_days if step_days is None else step_days
    if step < 1:
        raise ValueError("step_days must be at least 1")

    splits: list[Split] = []
    for index in range(folds):
        offset = dt.timedelta(days=step * (folds - 1 - index))
        split = time_split(
            start,
            end - offset,
            test_days=test_days,
            horizon_days=horizon_days,
            embargo_days=embargo_days,
            name=f"fold_{index + 1}",
        )
        splits.append(split)

    if not expanding:
        window = min(split.train_days for split in splits)
        splits = [
            Split(
                name=split.name,
                train_start=split.train_end - dt.timedelta(days=window - 1),
                train_end=split.train_end,
                test_start=split.test_start,
                test_end=split.test_end,
            )
            for split in splits
        ]
    return splits


def assert_embargo(splits: list[Split], *, minimum_days: int) -> None:
    """Fail loudly if any fold's embargo is narrower than ``minimum_days``.

    Call this after building splits and before training, passing the forecast
    horizon — or the longest rolling-feature window, if you want the stricter
    guarantee. It is the cheap assertion that catches the expensive mistake,
    and unlike a comment it survives someone editing the split parameters.
    """
    offenders = [split for split in splits if split.embargo_days < minimum_days]
    if offenders:
        raise ValueError(
            f"embargo narrower than the required {minimum_days} days in: "
            + "; ".join(split.describe() for split in offenders)
        )
