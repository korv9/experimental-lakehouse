import pytest

from lakehouse_platform.ml import metrics
from products.fashion_demand.ml import baselines


def row(**values):
    base = {
        "units_sold": None,
        "units_sold_lag_1": None,
        "units_sold_lag_7": None,
        "units_sold_lag_14": None,
        "units_sold_lag_28": None,
        "units_sold_mean_7d": None,
        "units_sold_mean_28d": None,
    }
    base.update(values)
    return base


def test_last_observed_uses_the_forecast_origin():
    assert baselines.last_observed(row(units_sold=12, units_sold_lag_1=99)) == 12.0


def test_last_observed_falls_back_when_the_origin_is_missing():
    assert baselines.last_observed(row(units_sold_lag_1=7)) == 7.0


def test_a_whole_number_of_weeks_makes_seasonal_naive_the_origin_value():
    """Horizon 14 lands on the same weekday as the origin, so no lag is needed."""
    prediction = baselines.seasonal_naive(row(units_sold=20, units_sold_lag_7=5), horizon_days=14)
    assert prediction == 20.0


def test_a_partial_week_steps_back_to_the_targets_weekday():
    # Horizon 10 = one week plus 3 days, so the matching weekday is 4 days back.
    prediction = baselines.seasonal_naive(
        row(units_sold=20, units_sold_lag_1=3), horizon_days=10
    )
    # No lag_4 column exists, so it falls through to the weekly lags, then zero.
    assert prediction == 0.0

    with_weekly = baselines.seasonal_naive(
        row(units_sold=20, units_sold_lag_7=9), horizon_days=10
    )
    assert with_weekly == 9.0


def test_seasonal_naive_rejects_a_meaningless_horizon():
    with pytest.raises(ValueError, match="at least 1"):
        baselines.seasonal_naive(row(units_sold=1), horizon_days=0)


def test_moving_average_prefers_the_widest_window_it_has():
    assert baselines.moving_average(row(units_sold_mean_28d=4.5, units_sold_mean_7d=9)) == 4.5
    assert baselines.moving_average(row(units_sold_mean_7d=9)) == 9.0


def test_a_cold_start_row_predicts_zero_rather_than_crashing():
    """A brand new article has sold nothing so far; that is the honest guess."""
    for model in baselines.baselines().values():
        assert model(row()) == 0.0


def test_the_baseline_set_is_named_for_the_registry():
    assert set(baselines.baselines()) == {
        "last_observed",
        "seasonal_naive",
        "moving_average_28d",
    }


def test_baselines_can_be_scored_and_ranked_end_to_end():
    """The comparison a model has to win, exercised with no Spark and no data."""
    rows = [
        row(units_sold=10, units_sold_mean_28d=2),
        row(units_sold=12, units_sold_mean_28d=3),
        row(units_sold=11, units_sold_mean_28d=2),
    ]
    actual = [10.0, 12.0, 11.0]

    scores = {
        name: metrics.evaluate(actual, baselines.predict(rows, model))
        for name, model in baselines.baselines().items()
    }
    ranked = metrics.compare(scores)

    # Demand is flat and weekday-driven here, so the naive baselines win and the
    # smoothed one loses — which is the point of running all three.
    assert ranked[0] in {"seasonal_naive", "last_observed"}
    assert ranked[-1] == "moving_average_28d"
    assert scores["seasonal_naive"]["wmape"] == 0.0
