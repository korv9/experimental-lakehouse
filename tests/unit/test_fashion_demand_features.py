"""Checks on the feature layer that do not need a Spark runtime.

The Spark transformation itself cannot be executed here, so these tests pin the
things that would otherwise only fail on a cluster: that the contract and the
transformation agree on the column list, that the guard rails are wired up, and
that nothing in the label or leakage logic can be quietly loosened.
"""
import ast
from pathlib import Path

import pytest

from lakehouse_platform.ml import features as ml_features
from products.fashion_demand.ml import dataset
from products.fashion_demand.tables.feature.demand_features import transform

ROOT = Path(__file__).resolve().parents[2]
TABLE = ROOT / "products/fashion_demand/tables/feature/demand_features"


def contract_columns() -> list[str]:
    """Read the contract's field list without importing it (no Spark needed)."""
    tree = ast.parse((TABLE / "contract.py").read_text(encoding="utf-8"))
    definition = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "TableDefinition"
    )
    return [
        node.target.id for node in definition.body if isinstance(node, ast.AnnAssign)
    ]


def selected_columns() -> set[str]:
    """The columns transform._select actually produces, derived from its config."""
    value = transform.VALUE
    columns = {"article_id", "sales_channel_id", "feature_date", value}
    columns |= {f"{value}_lag_{lag}" for lag in transform.LAGS}
    columns |= {
        f"{value}_{agg}_{days}d"
        for days in transform.ROLLING_WINDOWS
        for agg in ("mean", "stddev")
    }
    columns |= {"mean_unit_price_lag_1"}
    columns |= {"day_of_week", "day_of_month", "week_of_year", "month", "year", "is_weekend"}
    columns |= {"target", "horizon_days", "loaded_at"}
    return columns


def test_contract_and_transform_agree_on_every_column():
    """Adding a lag without adding it to the contract fails here, not on a cluster."""
    assert set(contract_columns()) == selected_columns()


def test_the_same_day_price_is_not_a_feature():
    """It is computed from the day's transactions, so it is a function of the target."""
    columns = contract_columns()
    assert "mean_unit_price" not in columns
    assert "mean_unit_price_lag_1" in columns


def test_no_lag_reaches_into_the_present():
    assert all(lag >= 1 for lag in transform.LAGS)


def test_the_declared_max_window_really_is_the_widest_one():
    assert transform.MAX_WINDOW_DAYS == max(transform.ROLLING_WINDOWS)


def test_the_quality_gate_pins_the_horizon_the_transform_builds():
    """A feature table mixing horizons averages two different questions."""
    import yaml

    rules = yaml.safe_load((TABLE / "quality.yaml").read_text(encoding="utf-8"))
    horizon = next(r for r in rules if r["name"] == "horizon_is_the_declared_one")
    arguments = horizon["check"]["arguments"]
    assert arguments["min_limit"] == arguments["max_limit"] == transform.HORIZON_DAYS
    assert horizon["criticality"] == "error"


def test_the_label_rules_reject_rather_than_warn():
    import yaml

    rules = yaml.safe_load((TABLE / "quality.yaml").read_text(encoding="utf-8"))
    by_name = {rule["name"]: rule for rule in rules}
    assert by_name["target_not_null"]["criticality"] == "error"
    assert by_name["target_not_negative"]["criticality"] == "error"
    # Missing history is a cold start, not a bad row.
    assert by_name["recent_history_available"]["criticality"] == "warn"


def test_the_model_matrix_excludes_identifiers_and_the_label():
    matrix = dataset.feature_columns(contract_columns())
    assert "target" not in matrix
    assert "horizon_days" not in matrix
    assert "article_id" not in matrix
    assert "feature_date" not in matrix
    assert "loaded_at" not in matrix
    # ...and still contains the features themselves.
    assert "units_sold_mean_28d" in matrix
    assert "mean_unit_price_lag_1" in matrix


def test_a_new_contract_column_joins_the_model_matrix_automatically():
    """Excluding by name means features opt in, not out — no annual audit."""
    assert "promo_flag" in dataset.feature_columns([*contract_columns(), "promo_flag"])


def test_platform_helpers_refuse_a_zero_lag():
    with pytest.raises(ValueError, match="at least 1 day"):
        ml_features.add_lags(None, value="x", keys=("k",), date_column="d", lags=(0,))


def test_platform_helpers_refuse_an_unknown_aggregation():
    with pytest.raises(ValueError, match="unsupported aggregations"):
        ml_features.add_rolling(
            None, value="x", keys=("k",), date_column="d",
            windows=(7,), aggregations=("median",),
        )


def test_a_panel_window_needs_an_entity_key():
    with pytest.raises(ValueError, match="entity key"):
        ml_features._window((), start=-7, end=-1)
