from __future__ import annotations

from dataclasses import asdict

import pytest

from lakehouse_platform.metadata.unity_catalog import UnityCatalogLayout
from lakehouse_platform.tools import databricks_demo
from lakehouse_platform.tools.databricks_demo import (
    DatabricksDemoOptions,
    DatabricksDemoReport,
    field_values,
    parse_bool,
)


class FakeRow:
    def __init__(self, **values):
        self.values = values

    def asDict(self, recursive=True):
        return self.values


@pytest.mark.parametrize("value", ["true", "TRUE", "1", "yes", "on", True])
def test_parse_bool_accepts_true_values(value):
    assert parse_bool(value) is True


@pytest.mark.parametrize("value", ["false", "FALSE", "0", "no", "off", False])
def test_parse_bool_accepts_false_values(value):
    assert parse_bool(value) is False


def test_parse_bool_rejects_ambiguous_values():
    with pytest.raises(ValueError, match="boolean"):
        parse_bool("perhaps")


def test_field_values_supports_databricks_column_name_variants():
    rows = [
        FakeRow(databaseName="bronze"),
        FakeRow(namespace="silver"),
        {"schemaName": "gold"},
        FakeRow(databaseName="bronze"),
    ]

    assert field_values(rows, "databaseName", "namespace", "schemaName") == [
        "bronze",
        "gold",
        "silver",
    ]


def test_full_demo_runs_steps_in_order_without_starting_product_ingestion(
    monkeypatch,
    capsys,
):
    calls = []

    def runtime(spark, catalog):
        calls.append(("runtime", catalog))
        return DatabricksDemoReport(
            catalog=catalog,
            spark_version="test-spark",
            current_user="portfolio@example.test",
        )

    def create_uc(spark, layout, *, create_catalog):
        calls.append(("unity_catalog", layout.catalog, create_catalog))

    def create_tables(spark, catalog):
        calls.append(("control_tables", catalog))

    def inspect(spark, layout, report):
        calls.append(("inventory", layout.catalog))
        report.schemas = list(layout.schemas)
        report.volumes = [layout.source_files_volume, layout.checkpoints_volume]
        report.control_tables = [
            "pipeline_runs",
            "ingestion_state",
            "ingestion_checkpoints",
            "download_manifest",
            "data_quality_results",
        ]

    def volume(layout):
        calls.append(("volume_probe", layout.catalog))
        return "a" * 64

    def control(spark, catalog):
        calls.append(("control_probe", catalog))
        return "run-123"

    def api(report):
        calls.append(("api_smoke", report.catalog))
        report.api_status = 200
        report.api_result_count = 4

    monkeypatch.setattr(databricks_demo, "_runtime_report", runtime)
    monkeypatch.setattr(databricks_demo, "create_unity_catalog_objects", create_uc)
    monkeypatch.setattr(databricks_demo, "_create_control_tables", create_tables)
    monkeypatch.setattr(databricks_demo, "_inspect_unity_catalog", inspect)
    monkeypatch.setattr(databricks_demo, "_volume_probe", volume)
    monkeypatch.setattr(databricks_demo, "_control_table_probe", control)
    monkeypatch.setattr(databricks_demo, "_api_smoke", api)

    report = databricks_demo.run_databricks_demo(
        object(),
        DatabricksDemoOptions(catalog="portfolio", create_catalog=False),
    )

    assert calls == [
        ("runtime", "portfolio"),
        ("unity_catalog", "portfolio", False),
        ("control_tables", "portfolio"),
        ("inventory", "portfolio"),
        ("volume_probe", "portfolio"),
        ("control_probe", "portfolio"),
        ("api_smoke", "portfolio"),
    ]
    assert asdict(report)["control_run_id"] == "run-123"
    output = capsys.readouterr().out
    assert "STEP 7: BOOTSTRAP SUMMARY" in output
    assert "does not start the Philosophy data-product ingestion" in output
    assert UnityCatalogLayout("portfolio").source_path("philosophy_litterature") in output


def test_demo_can_skip_mutating_volume_probe_and_external_api(monkeypatch, capsys):
    report = DatabricksDemoReport(
        catalog="portfolio",
        spark_version="test-spark",
        current_user="portfolio@example.test",
    )
    layout = UnityCatalogLayout("portfolio")

    monkeypatch.setattr(databricks_demo, "_runtime_report", lambda spark, catalog: report)
    monkeypatch.setattr(
        databricks_demo,
        "create_unity_catalog_objects",
        lambda spark, layout, create_catalog: None,
    )
    monkeypatch.setattr(
        databricks_demo,
        "_create_control_tables",
        lambda spark, catalog: None,
    )
    monkeypatch.setattr(
        databricks_demo,
        "_inspect_unity_catalog",
        lambda spark, layout, report: None,
    )
    monkeypatch.setattr(
        databricks_demo,
        "_control_table_probe",
        lambda spark, catalog: "run-123",
    )

    returned = databricks_demo.run_databricks_demo(
        object(),
        DatabricksDemoOptions(
            catalog=layout.catalog,
            run_volume_probe=False,
            run_api_smoke=False,
        ),
    )

    assert returned.volume_probe_sha256 is None
    assert returned.api_status is None
    output = capsys.readouterr().out
    assert "[SKIP] Volume write probe disabled." in output
    assert "[SKIP] Gutendex API smoke test disabled." in output

