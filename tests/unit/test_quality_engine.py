"""Behaviour of the single quality gate (Spark only where unavoidable)."""
from pathlib import Path

import pytest
import yaml

from lakehouse_platform.quality.engine import (
    CHECKS,
    CRITICALITIES,
    criticality_status,
    load_rules,
)


def _write_rules(tmp_path: Path, rules) -> Path:
    path = tmp_path / "quality.yaml"
    path.write_text(yaml.safe_dump(rules), encoding="utf-8")
    return path


def test_registry_exposes_the_supported_checks():
    assert set(CHECKS) == {"is_not_null", "is_in_range"}
    assert CRITICALITIES == {"error", "warn"}


def test_criticality_maps_to_a_result_status():
    assert criticality_status("error") == "fail"
    assert criticality_status("warn") == "warn"


def test_load_rules_reads_yaml(tmp_path):
    rules = [{"name": "r", "criticality": "warn",
              "check": {"function": "is_not_null", "arguments": {"column": "a"}}}]
    assert load_rules(_write_rules(tmp_path, rules)) == rules


def test_load_rules_tolerates_an_empty_file(tmp_path):
    path = tmp_path / "quality.yaml"
    path.write_text("", encoding="utf-8")
    assert load_rules(path) == []


# --- Spark-backed behaviour -------------------------------------------------
pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from lakehouse_platform.quality.engine import apply_quality  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("quality").getOrCreate()


@pytest.fixture()
def frame(spark):
    return spark.createDataFrame(
        [("a", 2000), (None, 2000), ("c", 9999), ("d", None)],
        ["record_id", "year"],
    )


def test_error_rules_reject_and_warn_rules_keep(tmp_path, frame):
    rules = [
        {"name": "id_not_null", "criticality": "error",
         "check": {"function": "is_not_null", "arguments": {"column": "record_id"}}},
        {"name": "year_range", "criticality": "warn",
         "check": {"function": "is_in_range",
                   "arguments": {"column": "year", "min_limit": 0, "max_limit": 2100}}},
    ]
    good, bad = apply_quality(frame, _write_rules(tmp_path, rules), "quarantine")

    # only the null id is rejected; the out-of-range year is warned about, not dropped
    assert sorted(row["record_id"] for row in good.collect()) == ["a", "c", "d"]
    assert [row["record_id"] for row in bad.collect()] == [None]


def test_no_row_is_lost_between_good_and_quarantine(tmp_path, frame):
    rules = [
        {"name": "year_not_null", "criticality": "error",
         "check": {"function": "is_not_null", "arguments": {"column": "year"}}},
    ]
    good, bad = apply_quality(frame, _write_rules(tmp_path, rules), "quarantine")
    assert good.count() + bad.count() == frame.count()


def test_fail_mode_raises_when_a_row_is_rejected(tmp_path, frame):
    rules = [
        {"name": "id_not_null", "criticality": "error",
         "check": {"function": "is_not_null", "arguments": {"column": "record_id"}}},
    ]
    with pytest.raises(ValueError, match="data-quality gate failed"):
        apply_quality(frame, _write_rules(tmp_path, rules), "fail")


def test_unsupported_function_is_rejected(tmp_path, frame):
    rules = [{"name": "nope", "criticality": "error",
              "check": {"function": "is_purple", "arguments": {"column": "record_id"}}}]
    with pytest.raises(ValueError, match="unsupported quality function"):
        apply_quality(frame, _write_rules(tmp_path, rules), "fail")
