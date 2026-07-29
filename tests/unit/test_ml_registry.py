from __future__ import annotations

import json

import pytest

from lakehouse_platform.ml.registry import ModelRun, create_ml_tables, record_model_run


class SparkRecorder:
    def __init__(self):
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)


def a_run(**overrides) -> ModelRun:
    defaults = dict(
        product="fashion_demand",
        model_name="lightgbm",
        feature_table="dev_lakehouse.feature.article_store_day",
        target="units_sold",
        horizon_days=14,
        train_start="2023-01-01",
        train_end="2023-11-01",
        test_start="2023-11-16",
        test_end="2023-12-13",
        metrics={"wmape": 0.19, "mae": 3.2},
        parameters={"num_leaves": 63, "learning_rate": 0.05},
    )
    defaults.update(overrides)
    return ModelRun(**defaults)


def test_setup_creates_both_ml_control_tables():
    spark = SparkRecorder()
    create_ml_tables(spark, "dev_lakehouse")

    joined = "\n".join(spark.statements)
    assert "dev_lakehouse.platform.ml_runs" in joined
    assert "dev_lakehouse.platform.ml_predictions" in joined
    # Re-runnable: the setup notebook calls this every deploy.
    assert joined.count("CREATE TABLE IF NOT EXISTS") == 2


def test_a_model_run_gets_an_id_even_when_the_caller_supplies_none():
    first, second = a_run(), a_run()
    assert first.model_run_id != second.model_run_id


def test_recording_a_run_stores_metrics_and_parameters_as_json():
    spark = SparkRecorder()
    run = a_run()

    returned = record_model_run(spark, "dev_lakehouse", run)

    assert returned == run.model_run_id
    statement = spark.statements[0]
    assert run.model_run_id in statement
    assert json.dumps(run.metrics, sort_keys=True) in statement
    assert json.dumps(run.parameters, sort_keys=True, default=str) in statement


def test_dates_are_written_as_dates_not_strings():
    spark = SparkRecorder()
    record_model_run(spark, "dev_lakehouse", a_run())
    assert "DATE'2023-01-01'" in spark.statements[0]


def test_a_missing_pipeline_run_id_is_a_sql_null_not_the_text_none():
    spark = SparkRecorder()
    record_model_run(spark, "dev_lakehouse", a_run(pipeline_run_id=None))
    statement = spark.statements[0]
    assert "'None'" not in statement
    assert "NULL" in statement


def test_quotes_in_a_model_name_cannot_break_out_of_the_statement():
    spark = SparkRecorder()
    record_model_run(spark, "dev_lakehouse", a_run(model_name="o'brien's model"))
    assert "'o''brien''s model'" in spark.statements[0]


def test_primary_metric_reads_the_headline_score():
    run = a_run()
    assert run.primary_metric() == 0.19
    assert run.primary_metric("mae") == 3.2
    assert run.primary_metric("rmse") is None


def test_write_predictions_rejects_a_frame_without_the_required_columns():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    from lakehouse_platform.ml.registry import write_predictions

    spark = SparkSession.builder.master("local[1]").appName("ml-registry").getOrCreate()
    frame = spark.createDataFrame([("store_1", 1.0)], "entity_id string, prediction double")

    with pytest.raises(ValueError, match=r"missing columns \['target_date'\]"):
        write_predictions(spark, "dev_lakehouse", frame, model_run_id="abc")
