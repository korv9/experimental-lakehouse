"""Data quality with DQX (Databricks Labs DQX).

DQX applies declarative rules to a DataFrame and splits it into rows that pass
and rows that fail ("quarantine"), so bad data never silently reaches silver.
Rules can live as config (see config/quality/example_works_checks.yaml), keeping
quality configuration-driven like sources.

Install:  pip install databricks-labs-dqx
NOTE: check-function names/arguments follow the installed DQX version; adjust if
the API differs. The rules are inlined here so the example is self-contained;
``load_checks_from_yaml`` shows the config-driven equivalent.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:  # keep this module importable without Spark (rules are just data)
    from pyspark.sql import DataFrame, SparkSession

# Rules as metadata (mirrors config/quality/example_works_checks.yaml):
#   error -> failing rows are quarantined;  warn -> row kept but flagged
WORKS_CHECKS = [
    {"name": "work_id_not_null", "criticality": "error",
     "check": {"function": "is_not_null", "arguments": {"column": "work_id"}}},
    {"name": "title_not_null", "criticality": "error",
     "check": {"function": "is_not_null", "arguments": {"column": "title"}}},
    {"name": "year_in_range", "criticality": "warn",
     "check": {"function": "is_in_range",
               "arguments": {"column": "year", "min_limit": 0, "max_limit": 2100}}},
]


def load_checks_from_yaml(path: str) -> list[dict]:
    """The config-driven alternative: load the same rules from a YAML file."""
    with open(path) as f:
        return yaml.safe_load(f)


def apply_quality(spark: SparkSession, df: DataFrame, *, table: str,
                  run_id: str | None, catalog: str) -> tuple[DataFrame, DataFrame]:
    """Return ``(good_df, quarantine_df)`` and persist a quality summary.

    Rows failing an 'error' rule go to quarantine; the rest pass through. Every
    run's outcome is written to platform.data_quality_results so quality can be
    compared run over run.
    """
    from databricks.labs.dqx.engine import DQEngine
    from databricks.sdk import WorkspaceClient

    engine = DQEngine(WorkspaceClient())
    good, quarantine = engine.apply_checks_by_metadata_and_split(df, WORKS_CHECKS)

    _persist_results(spark, catalog, run_id, table, good.count(), quarantine.count())
    return good, quarantine


def _persist_results(spark: SparkSession, catalog: str, run_id: str | None,
                     table: str, good_count: int, bad_count: int) -> None:
    from pyspark.sql import Row

    total = good_count + bad_count
    rate = (bad_count / total) if total else 0.0
    (spark.createDataFrame([Row(
        run_id=run_id, table_name=table, check_name="quarantine_rate",
        status=("fail" if rate > 0.5 else "pass"), metric=float(rate),
        threshold=0.5, checked_at=datetime.now(timezone.utc),
    )]).write.mode("append").saveAsTable(f"{catalog}.platform.data_quality_results"))
