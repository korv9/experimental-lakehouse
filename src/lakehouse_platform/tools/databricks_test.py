"""End-to-end platform test for Databricks, driven by the messy_records product.

This is the smoke test to run after deploying a platform change. It needs no
external API: the messy seed file ships with the repository, so the whole
Bronze -> Silver path is exercised from data under version control.

What it proves, in order:
    1. platform   catalog, schemas and control tables exist
    2. landing    the seed lands verbatim in append-only Bronze
    3. transform  cleaning, the quality gate and the contract gate all run
    4. verify     the tables hold exactly what the seed implies

Step 4 asserts real numbers rather than "it finished". The seed has 11 records:
one duplicate id collapses to 10, then two rows fail an error-level rule (a null
id and a null title) and are quarantined, leaving 8 in Silver. Those counts are
deterministic, so a drift in cleaning, dedup or the quality gate fails here.

Run it from ``databricks_test.py`` in the repository root, or as the
``platform_smoke_test`` job in orchestration.yml.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lakehouse_platform.jobs import process_job, read_table
from lakehouse_platform.metadata.control_tables import create_platform_tables
from lakehouse_platform.metadata.unity_catalog import (
    UnityCatalogLayout,
    create_unity_catalog_objects,
)

# Derived from datasets/messy_demo/raw_records.json — see the module docstring.
SEED_RECORDS = 11
EXPECTED_SILVER_ROWS = 8
EXPECTED_QUARANTINE_ROWS = 2

LAND_ACON = "products/messy_records/pipelines/land_bronze.yaml"
SILVER_ACON = "products/messy_records/pipelines/bronze_to_silver.yaml"
SEED_FILE = "datasets/messy_demo/raw_records.json"

BRONZE_TABLE = "bronze.messy_demo_records"
SILVER_TABLE = "silver.records"
QUARANTINE_TABLE = "quarantine.messy_records"


@dataclass(frozen=True)
class DatabricksTestOptions:
    catalog: str = "dev_lakehouse"
    repository_root: Path = Path()
    create_catalog: bool = True
    setup_platform: bool = True


@dataclass
class StepResult:
    name: str
    status: str
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.status == "PASS"


@dataclass
class DatabricksTestReport:
    steps: list[StepResult] = field(default_factory=list)

    def add(self, name: str, status: str, detail: str = "") -> StepResult:
        result = StepResult(name=name, status=status, detail=detail)
        self.steps.append(result)
        print(f"[{status:4}] {name}" + (f" — {detail}" if detail else ""))
        return result

    @property
    def passed(self) -> bool:
        return all(step.ok for step in self.steps)


def _table(catalog: str, name: str) -> str:
    return f"{catalog}.{name}"


def _count(spark: Any, catalog: str, name: str) -> int:
    return read_table(spark, name, catalog=catalog).count()


def _expect(report: DatabricksTestReport, name: str, condition: bool, detail: str) -> None:
    report.add(name, "PASS" if condition else "FAIL", detail)


def run_databricks_test(
    spark: Any,
    options: DatabricksTestOptions | None = None,
) -> DatabricksTestReport:
    options = options or DatabricksTestOptions()
    catalog = options.catalog
    root = Path(options.repository_root)
    report = DatabricksTestReport()

    print("=" * 78)
    print(f"PLATFORM TEST — messy_records end to end in {catalog}")
    print("=" * 78)

    # 1. platform ---------------------------------------------------------
    if options.setup_platform:
        layout = UnityCatalogLayout(catalog)
        create_unity_catalog_objects(spark, layout, create_catalog=options.create_catalog)
        create_platform_tables(spark, catalog)
        report.add("platform objects", "PASS", f"catalog and control tables ready in {catalog}")

    # 2. landing ----------------------------------------------------------
    land_run = process_job(
        spark,
        acon=str(root / LAND_ACON),
        catalog=catalog,
        variables={"source_file": str(root / SEED_FILE)},
    )
    report.add("landing run", "PASS", f"run_id={land_run}")

    # 3. transform --------------------------------------------------------
    silver_run = process_job(spark, acon=str(root / SILVER_ACON), catalog=catalog)
    report.add("bronze -> silver run", "PASS", f"run_id={silver_run}")

    # 4. verify -----------------------------------------------------------
    bronze_rows = _count(spark, catalog, BRONZE_TABLE)
    silver_rows = _count(spark, catalog, SILVER_TABLE)
    quarantine_rows = _count(spark, catalog, QUARANTINE_TABLE)

    _expect(
        report,
        "bronze landed the seed",
        bronze_rows >= SEED_RECORDS and bronze_rows % SEED_RECORDS == 0,
        f"{bronze_rows} rows (a multiple of {SEED_RECORDS}; Bronze appends per run)",
    )
    _expect(
        report,
        "silver deduplicated and cleaned",
        silver_rows == EXPECTED_SILVER_ROWS,
        f"{silver_rows} rows, expected {EXPECTED_SILVER_ROWS}",
    )
    _expect(
        report,
        "quality gate quarantined the bad rows",
        quarantine_rows >= EXPECTED_QUARANTINE_ROWS,
        f"{quarantine_rows} rows, expected at least {EXPECTED_QUARANTINE_ROWS}",
    )

    runs = read_table(spark, "platform.pipeline_runs", catalog=catalog)
    successful = runs.filter("status = 'success'").count()
    _expect(report, "runs recorded as metadata", successful >= 2,
            f"{successful} successful runs in platform.pipeline_runs")

    checks = read_table(spark, "platform.data_quality_results", catalog=catalog).count()
    _expect(report, "quality results persisted", checks >= 1,
            f"{checks} rows in platform.data_quality_results")

    print("-" * 78)
    print("RESULT:", "PASS — the platform works end to end" if report.passed else "FAIL")
    print("-" * 78)
    print("Inspect:")
    print(f"  SELECT * FROM {_table(catalog, SILVER_TABLE)};")
    print(f"  SELECT * FROM {_table(catalog, QUARANTINE_TABLE)};")
    print(f"  SELECT * FROM {_table(catalog, 'platform.pipeline_runs')} ORDER BY started_at DESC;")
    print(f"  SELECT * FROM {_table(catalog, 'platform.data_quality_results')};")

    if not report.passed:
        failed = ", ".join(step.name for step in report.steps if not step.ok)
        raise RuntimeError(f"platform test failed: {failed}")
    return report
