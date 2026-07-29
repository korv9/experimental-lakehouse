"""Forecast accuracy metrics — pure Python, so they are testable without Spark.

Retail demand is the motivating case, and it breaks the textbook defaults in two
ways worth stating explicitly:

* **Zero-demand days are normal.** Closed stores, sold-out sizes, seasonal
  products out of season. MAPE divides by the actual value, so a single zero
  makes it infinite. WMAPE — total absolute error over total actual volume — is
  the retail standard precisely because it is defined when actuals are zero and
  because it weights a big store's error more than a small one's.
* **Error direction has a price.** Under-forecasting loses a sale; over-
  forecasting funds a markdown. A model can have excellent MAE and still be
  systematically low, so ``bias`` is reported next to the magnitude metrics
  rather than left for someone to notice later.

Every function takes any two sequences of numbers, so pandas Series, numpy
arrays and plain lists all work.
"""
from __future__ import annotations

import math
from collections.abc import Sequence


def _paired(actual: Sequence[float], predicted: Sequence[float]) -> list[tuple[float, float]]:
    if len(actual) != len(predicted):
        raise ValueError(f"length mismatch: {len(actual)} actuals vs {len(predicted)} predictions")
    if not actual:
        raise ValueError("cannot score an empty series")
    return [(float(a), float(p)) for a, p in zip(actual, predicted)]


def mean_absolute_error(actual: Sequence[float], predicted: Sequence[float]) -> float:
    """Average absolute error, in units of the target. The default to report."""
    pairs = _paired(actual, predicted)
    return sum(abs(a - p) for a, p in pairs) / len(pairs)


def root_mean_squared_error(actual: Sequence[float], predicted: Sequence[float]) -> float:
    """Like MAE but penalises large misses quadratically — sensitive to spikes."""
    pairs = _paired(actual, predicted)
    return math.sqrt(sum((a - p) ** 2 for a, p in pairs) / len(pairs))


def weighted_mean_absolute_percentage_error(
    actual: Sequence[float], predicted: Sequence[float]
) -> float:
    """Total absolute error / total actual volume.

    Defined whenever the actuals do not sum to zero, which makes it the metric
    to lead with on intermittent retail demand. Returned as a fraction (0.12,
    not 12%) so downstream formatting stays a presentation decision.
    """
    pairs = _paired(actual, predicted)
    total = sum(abs(a) for a, _ in pairs)
    if total == 0:
        raise ValueError("WMAPE is undefined when every actual is zero")
    return sum(abs(a - p) for a, p in pairs) / total


def mean_absolute_percentage_error(
    actual: Sequence[float], predicted: Sequence[float]
) -> tuple[float, float]:
    """MAPE over the non-zero actuals, plus the fraction of rows it covers.

    Returning coverage is the point. A MAPE computed over the 40% of rows that
    happen to be non-zero is not comparable to one computed over 95%, and
    reporting the number alone hides that.
    """
    pairs = [(a, p) for a, p in _paired(actual, predicted) if a != 0]
    coverage = len(pairs) / len(actual)
    if not pairs:
        return float("nan"), coverage
    return sum(abs(a - p) / abs(a) for a, p in pairs) / len(pairs), coverage


def bias(actual: Sequence[float], predicted: Sequence[float]) -> float:
    """Mean signed error. Positive means the model over-forecasts on average."""
    pairs = _paired(actual, predicted)
    return sum(p - a for a, p in pairs) / len(pairs)


def evaluate(actual: Sequence[float], predicted: Sequence[float]) -> dict[str, float]:
    """The full metric set, ready to store as JSON on a model run."""
    mape, coverage = mean_absolute_percentage_error(actual, predicted)
    return {
        "mae": mean_absolute_error(actual, predicted),
        "rmse": root_mean_squared_error(actual, predicted),
        "wmape": weighted_mean_absolute_percentage_error(actual, predicted),
        "mape": mape,
        "mape_coverage": coverage,
        "bias": bias(actual, predicted),
        "count": float(len(actual)),
    }


def compare(results: dict[str, dict[str, float]], *, metric: str = "wmape") -> list[str]:
    """Rank named models best-first on one metric.

    A baseline that a model cannot beat is the most useful result an experiment
    can produce, so ranking is a first-class operation rather than something
    eyeballed in a notebook cell.
    """
    if not results:
        raise ValueError("nothing to compare")
    missing = [name for name, scores in results.items() if metric not in scores]
    if missing:
        raise ValueError(f"models {sorted(missing)} have no '{metric}' score")
    return sorted(results, key=lambda name: results[name][metric])
