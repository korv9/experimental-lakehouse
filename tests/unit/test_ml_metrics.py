import math

import pytest

from lakehouse_platform.ml import metrics


def test_perfect_forecast_scores_zero_error():
    actual = [10.0, 20.0, 30.0]
    scores = metrics.evaluate(actual, actual)
    assert scores["mae"] == 0.0
    assert scores["rmse"] == 0.0
    assert scores["wmape"] == 0.0
    assert scores["bias"] == 0.0


def test_mae_and_rmse_differ_when_one_miss_is_large():
    actual = [10.0, 10.0, 10.0, 10.0]
    predicted = [10.0, 10.0, 10.0, 50.0]
    assert metrics.mean_absolute_error(actual, predicted) == 10.0
    # RMSE punishes the single big miss harder than MAE does.
    assert metrics.root_mean_squared_error(actual, predicted) == 20.0


def test_wmape_is_defined_when_some_actuals_are_zero():
    """The whole reason WMAPE leads on retail demand: closed-store days."""
    actual = [0.0, 0.0, 100.0]
    predicted = [5.0, 5.0, 90.0]
    assert metrics.weighted_mean_absolute_percentage_error(actual, predicted) == 0.2


def test_wmape_rejects_an_all_zero_series_instead_of_dividing_by_zero():
    with pytest.raises(ValueError, match="undefined"):
        metrics.weighted_mean_absolute_percentage_error([0.0, 0.0], [1.0, 2.0])


def test_mape_reports_how_much_of_the_series_it_actually_covered():
    actual = [0.0, 0.0, 0.0, 100.0]
    predicted = [1.0, 1.0, 1.0, 90.0]
    mape, coverage = metrics.mean_absolute_percentage_error(actual, predicted)
    assert mape == pytest.approx(0.1)
    assert coverage == 0.25


def test_mape_is_nan_rather_than_a_lie_when_every_actual_is_zero():
    mape, coverage = metrics.mean_absolute_percentage_error([0.0, 0.0], [1.0, 1.0])
    assert math.isnan(mape)
    assert coverage == 0.0


def test_bias_separates_a_systematically_low_model_from_an_accurate_one():
    actual = [100.0, 100.0, 100.0, 100.0]
    low = [90.0, 90.0, 90.0, 90.0]
    noisy = [90.0, 110.0, 90.0, 110.0]

    # Identical MAE...
    assert metrics.mean_absolute_error(actual, low) == metrics.mean_absolute_error(actual, noisy)
    # ...but only one of them will systematically understock the shelf.
    assert metrics.bias(actual, low) == -10.0
    assert metrics.bias(actual, noisy) == 0.0


def test_length_mismatch_is_an_error_not_a_silent_zip():
    with pytest.raises(ValueError, match="length mismatch"):
        metrics.mean_absolute_error([1.0, 2.0], [1.0])


def test_empty_series_is_an_error():
    with pytest.raises(ValueError, match="empty"):
        metrics.mean_absolute_error([], [])


def test_compare_ranks_models_best_first():
    results = {
        "seasonal_naive": {"wmape": 0.28},
        "lightgbm": {"wmape": 0.19},
        "linear": {"wmape": 0.24},
    }
    assert metrics.compare(results) == ["lightgbm", "linear", "seasonal_naive"]


def test_compare_refuses_a_metric_a_model_never_reported():
    results = {"a": {"wmape": 0.1}, "b": {"mae": 3.0}}
    with pytest.raises(ValueError, match="no 'wmape' score"):
        metrics.compare(results)
