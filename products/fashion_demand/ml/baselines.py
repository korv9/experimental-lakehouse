"""Baselines the gradient-boosted model has to beat to be worth deploying.

A demand forecast is not judged against zero, it is judged against what the
planning team already does — which, in practice, is "roughly the same as last
week". A model that scores WMAPE 0.22 sounds fine until seasonal naive scores
0.21 on the same rows, at which point the honest result is that the model added
nothing. Running these first, on the same folds, is what makes the comparison
mean anything.

All three take a feature row as a mapping and return a prediction, so they
consume exactly the same table the trained model does — no separate data path
that could quietly disagree. They are pure Python and run in CI.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence

Row = Mapping[str, object]

SEASON_DAYS = 7


def _number(row: Row, column: str) -> float | None:
    value = row.get(column)
    return None if value is None else float(value)


def _first_available(row: Row, columns: Sequence[str]) -> float:
    """First non-null of several columns, falling back to zero.

    Zero is the right fallback here and only here: an article with no history at
    all has, so far, sold nothing. The feature-level rules record how often this
    happens as a cold-start rate.
    """
    for column in columns:
        value = _number(row, column)
        if value is not None:
            return value
    return 0.0


def last_observed(row: Row) -> float:
    """Predict whatever the series did on the forecast origin. The crudest bar."""
    return _first_available(row, ("units_sold", "units_sold_lag_1", "units_sold_lag_7"))


def seasonal_naive(row: Row, *, horizon_days: int = 14) -> float:
    """Predict the same weekday's value, stepping back whole weeks from the origin.

    For a horizon that is a multiple of 7 the target weekday equals the origin's
    weekday, so this is ``units_sold`` at the origin. Otherwise it walks back to
    the most recent lag that shares the target's weekday. This is the baseline
    that is genuinely hard to beat on retail data, because weekday effects are
    the largest single component of the signal.
    """
    if horizon_days < 1:
        raise ValueError("horizon_days must be at least 1")

    offset = horizon_days % SEASON_DAYS
    # Lag columns the feature table actually carries, nearest first.
    candidates = ["units_sold"] if offset == 0 else [f"units_sold_lag_{SEASON_DAYS - offset}"]
    candidates += [f"units_sold_lag_{lag}" for lag in (7, 14, 28) if lag not in candidates]
    return _first_available(row, candidates)


def moving_average(row: Row, *, window_days: int = 28) -> float:
    """Predict the mean of the recent past. Smooth, unbiased, ignores seasonality.

    Usually loses to seasonal naive on weekday-driven demand and beats it on
    intermittent, low-volume articles — which is itself a useful finding, and an
    argument for segmenting the evaluation rather than reporting one number.
    """
    return _first_available(
        row,
        (f"units_sold_mean_{window_days}d", "units_sold_mean_7d", "units_sold", "units_sold_lag_1"),
    )


def baselines(horizon_days: int = 14) -> dict[str, Callable[[Row], float]]:
    """The standard set, named for the model registry and the comparison table."""
    return {
        "last_observed": last_observed,
        "seasonal_naive": lambda row: seasonal_naive(row, horizon_days=horizon_days),
        "moving_average_28d": lambda row: moving_average(row, window_days=28),
    }


def predict(rows: Sequence[Row], model: Callable[[Row], float]) -> list[float]:
    """Score every row with one baseline."""
    return [model(row) for row in rows]
