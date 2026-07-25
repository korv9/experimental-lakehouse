"""Verbose, idempotent first-run validation for a Databricks workspace."""
from __future__ import annotations

import hashlib
import platform
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lakehouse_platform.metadata.unity_catalog import (
    UnityCatalogLayout,
    create_unity_catalog_objects,
)
from lakehouse_platform.observability.progress import progress


@dataclass(frozen=True)
class DatabricksDemoOptions:
    catalog: str = "dev_lakehouse"
    create_catalog: bool = True
    run_volume_probe: bool = True
    run_api_smoke: bool = True


@dataclass
class DatabricksDemoReport:
    catalog: str
    spark_version: str
    current_user: str
    schemas: list[str] = field(default_factory=list)
    volumes: list[str] = field(default_factory=list)
    control_tables: list[str] = field(default_factory=list)
    volume_probe_sha256: str | None = None
    control_run_id: str | None = None
    api_status: int | None = None
    api_result_count: int | None = None
    warnings: list[str] = field(default_factory=list)


def parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Expected a boolean value, received {value!r}")


def _heading(number: int, title: str) -> None:
    print()
    print("=" * 88)
    print(f"STEP {number}: {title}")
    print("=" * 88)


def _row_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    if isinstance(row, dict):
        return row
    raise TypeError(f"Cannot convert row of type {type(row).__name__} to a dictionary")


def field_values(rows: list[Any], *candidate_fields: str) -> list[str]:
    values: list[str] = []
    for row in rows:
        document = _row_dict(row)
        value = next(
            (document[field] for field in candidate_fields if document.get(field) is not None),
            None,
        )
        if value is not None:
            values.append(str(value))
    return sorted(set(values))


def _scalar_row(spark: Any, query: str) -> dict[str, Any]:
    rows = spark.sql(query).collect()
    if not rows:
        raise RuntimeError(f"Query returned no rows: {query}")
    return _row_dict(rows[0])


def _runtime_report(spark: Any, catalog: str) -> DatabricksDemoReport:
    identity = _scalar_row(
        spark,
        "SELECT current_user() AS current_user, current_catalog() AS current_catalog",
    )
    spark_version = str(getattr(spark, "version", "unknown"))
    progress(
        "DATABRICKS",
        "Runtime detected",
        python=platform.python_version(),
        spark=spark_version,
        current_user=identity["current_user"],
        current_catalog=identity["current_catalog"],
        target_catalog=catalog,
    )
    return DatabricksDemoReport(
        catalog=catalog,
        spark_version=spark_version,
        current_user=str(identity["current_user"]),
    )


def _inspect_unity_catalog(
    spark: Any,
    layout: UnityCatalogLayout,
    report: DatabricksDemoReport,
) -> None:
    report.schemas = field_values(
        spark.sql(f"SHOW SCHEMAS IN {layout.catalog}").collect(),
        "databaseName",
        "namespace",
        "schemaName",
    )
    report.volumes = field_values(
        spark.sql(
            f"SHOW VOLUMES IN {layout.catalog}.{layout.landing_schema}"
        ).collect()
        + spark.sql(
            f"SHOW VOLUMES IN {layout.catalog}.{layout.platform_schema}"
        ).collect(),
        "volume_name",
        "volumeName",
        "name",
    )
    report.control_tables = field_values(
        spark.sql(f"SHOW TABLES IN {layout.catalog}.{layout.platform_schema}").collect(),
        "tableName",
        "table_name",
    )

    expected_schemas = set(layout.schemas)
    expected_volumes = {layout.source_files_volume, layout.checkpoints_volume}
    expected_tables = {
        "pipeline_runs",
        "ingestion_state",
        "ingestion_checkpoints",
        "download_manifest",
        "data_quality_results",
    }
    missing = {
        "schemas": sorted(expected_schemas - set(report.schemas)),
        "volumes": sorted(expected_volumes - set(report.volumes)),
        "tables": sorted(expected_tables - set(report.control_tables)),
    }
    missing = {key: values for key, values in missing.items() if values}
    if missing:
        raise RuntimeError(f"Unity Catalog bootstrap is incomplete: {missing}")

    print("Schemas:")
    for schema in report.schemas:
        print(f"  [OK] {layout.catalog}.{schema}")
    print("Managed volumes:")
    print(
        f"  [OK] {layout.catalog}.{layout.landing_schema}."
        f"{layout.source_files_volume}"
    )
    print(
        f"  [OK] {layout.catalog}.{layout.platform_schema}."
        f"{layout.checkpoints_volume}"
    )
    print("Platform control tables:")
    for table in report.control_tables:
        print(f"  [OK] {layout.catalog}.{layout.platform_schema}.{table}")


def _volume_probe(layout: UnityCatalogLayout) -> str:
    path = Path(layout.source_path("shared", "_bootstrap_probe.txt"))
    payload = (
        "experimental-lakehouse Databricks bootstrap probe\n"
        f"catalog={layout.catalog}\n"
    ).encode()
    expected = hashlib.sha256(payload).hexdigest()
    progress("DATABRICKS", "Writing governed volume probe", path=path)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_bytes(payload)
        observed = hashlib.sha256(path.read_bytes()).hexdigest()
        if observed != expected:
            raise RuntimeError(
                f"Volume probe checksum mismatch: expected {expected}, observed {observed}"
            )
        progress("DATABRICKS", "Volume probe verified", sha256=observed)
        return observed
    finally:
        path.unlink(missing_ok=True)
        progress("DATABRICKS", "Volume probe cleaned up", path=path)


def _control_table_probe(spark: Any, catalog: str) -> str:
    # Lazy import keeps this module importable in local environments without PySpark.
    from lakehouse_platform.metadata.control_tables import finish_run, start_run

    run_id = start_run(
        spark,
        catalog,
        pipeline_name="demo_databricks_bootstrap",
        source_name="platform_self_test",
    )
    progress("DATABRICKS", "Control-table run opened", run_id=run_id)
    finish_run(spark, catalog, run_id, status="success", read=1, written=1)
    row = _scalar_row(
        spark,
        "SELECT run_id, status, records_written "
        f"FROM {catalog}.platform.pipeline_runs WHERE run_id = '{run_id}'",
    )
    if row.get("status") != "success" or row.get("records_written") != 1:
        raise RuntimeError(f"Control-table round trip failed: {row}")
    progress("DATABRICKS", "Control-table run verified", run_id=run_id, status="success")
    return run_id


def _create_control_tables(spark: Any, catalog: str) -> None:
    # Lazy import keeps local tooling usable when PySpark is not installed.
    from lakehouse_platform.metadata.control_tables import create_platform_tables

    create_platform_tables(spark, catalog)


def _api_smoke(report: DatabricksDemoReport) -> None:
    try:
        # Keep API-only dependencies optional for the Unity Catalog bootstrap.
        from lakehouse_platform.tools.api_explorer import ApiRequest, execute_request

        request = ApiRequest(
            name="gutendex_plato_smoke",
            url="https://gutendex.com/books/",
            params={"search": "plato"},
            headers={"User-Agent": "AtlasOfHumanThought/0.1 (Databricks bootstrap)"},
            timeout=30,
        )
        response = execute_request(request)
        report.api_status = response.status_code
        if not response.ok or not isinstance(response.body, dict):
            raise RuntimeError(
                f"Gutendex returned status={response.status_code} "
                f"body_type={type(response.body).__name__}"
            )
        report.api_result_count = int(response.body.get("count", 0))
        samples = [
            item.get("title")
            for item in response.body.get("results", [])[:3]
            if isinstance(item, dict)
        ]
        progress(
            "DATABRICKS",
            "External API access verified",
            status=response.status_code,
            result_count=report.api_result_count,
            sample_titles=samples,
        )
    except (ImportError, OSError, ValueError, RuntimeError) as error:
        warning = (
            "Gutendex smoke test failed. Unity Catalog setup is still usable, "
            f"but the compute may lack internet egress: {error}"
        )
        report.warnings.append(warning)
        progress("DATABRICKS", "API smoke test warning", error=str(error))


def _summary(report: DatabricksDemoReport, layout: UnityCatalogLayout) -> None:
    _heading(7, "BOOTSTRAP SUMMARY")
    print(f"  [OK] Spark runtime:       {report.spark_version}")
    print(f"  [OK] Current user:        {report.current_user}")
    print(f"  [OK] Target catalog:      {report.catalog}")
    print(f"  [OK] Schemas discovered:  {len(report.schemas)}")
    print(f"  [OK] Volumes discovered:  {len(report.volumes)}")
    print(f"  [OK] Control tables:      {len(report.control_tables)}")
    if report.volume_probe_sha256:
        print(f"  [OK] Volume read/write:   {report.volume_probe_sha256[:16]}...")
    if report.control_run_id:
        print(f"  [OK] Delta audit run:     {report.control_run_id}")
    if report.api_status:
        print(
            f"  [OK] Gutendex API:        HTTP {report.api_status}, "
            f"{report.api_result_count} matching works"
        )
    for warning in report.warnings:
        print(f"  [WARN] {warning}")

    print()
    print("Platform bootstrap completed.")
    print(f"Raw-file root: {layout.source_path('philosophy_litterature')}")
    print(
        "Next safe step: review IDEAS.md and the Philosophy product manifest. "
        "The product ingestion is intentionally not started by this bootstrap."
    )


def run_databricks_demo(
    spark: Any,
    options: DatabricksDemoOptions | None = None,
) -> DatabricksDemoReport:
    """Create and validate the platform foundation with visible progress."""
    options = options or DatabricksDemoOptions()
    layout = UnityCatalogLayout(options.catalog)

    print("=" * 88)
    print("EXPERIMENTAL LAKEHOUSE — DATABRICKS FIRST-RUN DEMO")
    print("=" * 88)
    print("This run is idempotent. It sets up platform objects and performs smoke tests.")
    print("It does not start the Philosophy data-product ingestion.")
    print(
        "Options: "
        f"catalog={options.catalog}, "
        f"create_catalog={options.create_catalog}, "
        f"run_volume_probe={options.run_volume_probe}, "
        f"run_api_smoke={options.run_api_smoke}"
    )

    _heading(1, "RUNTIME AND IDENTITY")
    report = _runtime_report(spark, options.catalog)

    _heading(2, "UNITY CATALOG SCHEMAS AND MANAGED VOLUMES")
    try:
        create_unity_catalog_objects(
            spark,
            layout,
            create_catalog=options.create_catalog,
        )
    except Exception as error:
        print()
        print("[FAILED] Could not create Unity Catalog objects.")
        print(f"Reason: {error}")
        print(
            "Required setup identity permissions include USE CATALOG plus schema/"
            "volume creation rights. If an administrator already created the catalog, "
            "set create_catalog=false."
        )
        raise

    _heading(3, "DELTA CONTROL TABLES")
    _create_control_tables(spark, options.catalog)
    progress("DATABRICKS", "Delta control tables ensured", catalog=options.catalog)

    _heading(4, "UNITY CATALOG INVENTORY VALIDATION")
    _inspect_unity_catalog(spark, layout, report)

    _heading(5, "GOVERNED STORAGE AND DELTA WRITE PROBES")
    if options.run_volume_probe:
        report.volume_probe_sha256 = _volume_probe(layout)
    else:
        print("[SKIP] Volume write probe disabled.")
    report.control_run_id = _control_table_probe(spark, options.catalog)

    _heading(6, "EXTERNAL API CONNECTIVITY")
    if options.run_api_smoke:
        _api_smoke(report)
    else:
        print("[SKIP] Gutendex API smoke test disabled.")

    _summary(report, layout)
    return report
