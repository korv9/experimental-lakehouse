"""Model runs as control tables: an experiment is queryable, not a printed cell.

The platform already treats metadata as data — ``platform.pipeline_runs``,
``platform.data_quality_results``. Model training gets the same treatment for
the same reason: "which model is live, trained on what, scoring how well" is a
question someone will ask months later, and a notebook output cannot answer it.

Two tables:

``platform.ml_runs``         one row per training run: data window, parameters,
                             metrics, and the ``run_id`` of the ACON pipeline
                             that built its features.
``platform.ml_predictions``  one row per scored entity-date, tagged with the
                             model run that produced it, so a forecast can
                             always be traced back to the model and the data.

Predictions land with ``actual`` null and are backfilled once the truth arrives.
That single column is what turns the table into a live accuracy monitor: joining
predictions to outcomes is how you notice a model degrading before a stakeholder
does.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def _sql_string(value: str | None) -> str:
    return "NULL" if value is None else "'" + value.replace("'", "''") + "'"


@dataclass(frozen=True)
class ModelRun:
    """Everything needed to reproduce a training run and judge its result."""

    product: str
    model_name: str
    feature_table: str
    target: str
    horizon_days: int
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    metrics: dict[str, float] = field(default_factory=dict)
    parameters: dict[str, object] = field(default_factory=dict)
    pipeline_run_id: str | None = None
    code_version: str | None = None
    model_run_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def primary_metric(self, metric: str = "wmape") -> float | None:
        return self.metrics.get(metric)


def create_ml_tables(spark: SparkSession, catalog: str) -> None:
    """Create the ML control tables if absent. Safe to re-run."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.platform")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.platform.ml_runs (
            model_run_id STRING, pipeline_run_id STRING, product STRING,
            model_name STRING, feature_table STRING, target STRING,
            horizon_days INT, train_start DATE, train_end DATE,
            test_start DATE, test_end DATE, parameters STRING, metrics STRING,
            code_version STRING, trained_at TIMESTAMP)
        USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.platform.ml_predictions (
            model_run_id STRING, entity_id STRING, target_date DATE,
            prediction DOUBLE, actual DOUBLE, predicted_at TIMESTAMP)
        USING DELTA
    """)


def record_model_run(spark: SparkSession, catalog: str, run: ModelRun) -> str:
    """Append a training run and return its ``model_run_id``."""
    spark.sql(f"""
        INSERT INTO {catalog}.platform.ml_runs (
            model_run_id, pipeline_run_id, product, model_name, feature_table,
            target, horizon_days, train_start, train_end, test_start, test_end,
            parameters, metrics, code_version, trained_at
        ) VALUES (
            {_sql_string(run.model_run_id)}, {_sql_string(run.pipeline_run_id)},
            {_sql_string(run.product)}, {_sql_string(run.model_name)},
            {_sql_string(run.feature_table)}, {_sql_string(run.target)},
            {int(run.horizon_days)},
            DATE{_sql_string(run.train_start)}, DATE{_sql_string(run.train_end)},
            DATE{_sql_string(run.test_start)}, DATE{_sql_string(run.test_end)},
            {_sql_string(json.dumps(run.parameters, sort_keys=True, default=str))},
            {_sql_string(json.dumps(run.metrics, sort_keys=True))},
            {_sql_string(run.code_version)}, current_timestamp()
        )
    """)
    return run.model_run_id


def write_predictions(
    spark: SparkSession,
    catalog: str,
    predictions: DataFrame,
    *,
    model_run_id: str,
) -> None:
    """Append scored rows, tagged with the run that produced them.

    ``predictions`` must carry ``entity_id``, ``target_date`` and ``prediction``.
    ``actual`` is optional and stays null until it is known.
    """
    from pyspark.sql import functions as F

    required = {"entity_id", "target_date", "prediction"}
    missing = required - set(predictions.columns)
    if missing:
        raise ValueError(f"predictions are missing columns {sorted(missing)}")

    frame = predictions
    if "actual" not in frame.columns:
        frame = frame.withColumn("actual", F.lit(None).cast("double"))

    (
        frame.withColumn("model_run_id", F.lit(model_run_id))
        .withColumn("predicted_at", F.current_timestamp())
        .select(
            "model_run_id", "entity_id", "target_date", "prediction", "actual", "predicted_at"
        )
        .write.mode("append")
        .saveAsTable(f"{catalog}.platform.ml_predictions")
    )


def backfill_actuals(
    spark: SparkSession,
    catalog: str,
    *,
    source_table: str,
    entity_column: str,
    date_column: str,
    value_column: str,
) -> None:
    """Fill in ``actual`` on past predictions from whatever table holds the truth.

    Run this on a schedule after the Gold table refreshes. Once it has run,
    ``platform.ml_predictions`` answers "how is the live model doing this week"
    with a single GROUP BY, with no retraining and no notebook.
    """
    spark.sql(f"""
        MERGE INTO {catalog}.platform.ml_predictions p
        USING (
            SELECT CAST({entity_column} AS STRING) AS entity_id,
                   {date_column} AS target_date,
                   CAST({value_column} AS DOUBLE) AS actual
            FROM {source_table}
        ) s
        ON p.entity_id = s.entity_id AND p.target_date = s.target_date
        WHEN MATCHED AND p.actual IS NULL THEN UPDATE SET actual = s.actual
    """)
