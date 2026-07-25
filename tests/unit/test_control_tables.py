from __future__ import annotations

import pytest


class SparkRecorder:
    def __init__(self):
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)


def test_start_run_uses_typed_sql_nulls_and_escapes_values():
    pytest.importorskip("pyspark")
    from lakehouse_platform.metadata.control_tables import start_run

    spark = SparkRecorder()

    run_id = start_run(
        spark,
        "dev_lakehouse",
        pipeline_name="author's_pipeline",
        source_name="source",
    )

    statement = spark.statements[0]
    assert run_id in statement
    assert "'author''s_pipeline'" in statement
    assert "current_timestamp(), NULL, 'running', NULL, NULL, NULL, NULL" in statement
    assert "createDataFrame" not in statement
