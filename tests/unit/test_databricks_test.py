"""The Databricks platform test, exercised with fakes (no Spark required).

The point of these is that the smoke test must *fail* when the data is wrong.
A test that only proves "the run finished" would pass through a broken cleaning
step, so the expectations below check both directions.
"""
from pathlib import Path

import pytest

from lakehouse_platform.tools import databricks_test as dbt
from lakehouse_platform.tools.databricks_test import (
    EXPECTED_QUARANTINE_ROWS,
    EXPECTED_SILVER_ROWS,
    SEED_RECORDS,
    DatabricksTestOptions,
    run_databricks_test,
)

ROOT = Path(__file__).resolve().parents[2]

HEALTHY = {
    "bronze.messy_demo_records": SEED_RECORDS,
    "silver.records": EXPECTED_SILVER_ROWS,
    "quarantine.messy_records": EXPECTED_QUARANTINE_ROWS,
    "platform.pipeline_runs": 2,
    "platform.data_quality_results": 4,
}


class _Frame:
    def __init__(self, rows):
        self._rows = rows

    def count(self):
        return self._rows

    def filter(self, _condition):
        return self  # every fake pipeline_runs row counts as successful


def _install_fakes(monkeypatch, counts):
    calls = {"process_job": []}

    def fake_read_table(spark, table, *, catalog, **kwargs):
        return _Frame(counts[table])

    def fake_process_job(spark, *, acon, catalog, variables=None, **kwargs):
        calls["process_job"].append(Path(acon).name)
        return f"run-{len(calls['process_job'])}"

    monkeypatch.setattr(dbt, "read_table", fake_read_table)
    monkeypatch.setattr(dbt, "process_job", fake_process_job)
    return calls


def _options():
    return DatabricksTestOptions(repository_root=ROOT, setup_platform=False)


def test_passes_on_the_expected_seed_outcome(monkeypatch, capsys):
    calls = _install_fakes(monkeypatch, HEALTHY)
    report = run_databricks_test(spark=None, options=_options())

    assert report.passed
    # it runs the landing pipeline before the transformation, in that order
    assert calls["process_job"] == ["land_bronze.yaml", "bronze_to_silver.yaml"]
    assert "PASS — the platform works end to end" in capsys.readouterr().out


def test_fails_when_silver_row_count_drifts(monkeypatch):
    _install_fakes(monkeypatch, {**HEALTHY, "silver.records": EXPECTED_SILVER_ROWS + 1})
    with pytest.raises(RuntimeError, match="silver deduplicated and cleaned"):
        run_databricks_test(spark=None, options=_options())


def test_fails_when_nothing_was_quarantined(monkeypatch):
    """The seed contains bad rows; zero quarantined means the gate stopped working."""
    _install_fakes(monkeypatch, {**HEALTHY, "quarantine.messy_records": 0})
    with pytest.raises(RuntimeError, match="quality gate quarantined"):
        run_databricks_test(spark=None, options=_options())


def test_fails_when_bronze_is_not_a_whole_number_of_landings(monkeypatch):
    _install_fakes(monkeypatch, {**HEALTHY, "bronze.messy_demo_records": SEED_RECORDS + 3})
    with pytest.raises(RuntimeError, match="bronze landed the seed"):
        run_databricks_test(spark=None, options=_options())


def test_accepts_bronze_after_repeated_landings(monkeypatch):
    """Bronze appends, so a second run doubles it while Silver stays deduplicated."""
    _install_fakes(monkeypatch, {**HEALTHY, "bronze.messy_demo_records": SEED_RECORDS * 2})
    assert run_databricks_test(spark=None, options=_options()).passed


def test_fails_when_quality_results_were_not_persisted(monkeypatch):
    _install_fakes(monkeypatch, {**HEALTHY, "platform.data_quality_results": 0})
    with pytest.raises(RuntimeError, match="quality results persisted"):
        run_databricks_test(spark=None, options=_options())


def test_expected_counts_match_the_checked_in_seed():
    """The constants must stay tied to the dataset they were derived from."""
    import json

    from lakehouse_platform.transforms.cleaning import clean_record

    records = json.loads((ROOT / dbt.SEED_FILE).read_text(encoding="utf-8"))
    assert len(records) == SEED_RECORDS

    deduped = {clean_record(r)["record_id"]: clean_record(r) for r in records}
    good = [r for r in deduped.values() if r["record_id"] and r["title"]]
    assert len(good) == EXPECTED_SILVER_ROWS
    assert len(deduped) - len(good) == EXPECTED_QUARANTINE_ROWS
