"""Control tables (``platform.*``): metadata is data.

Operational state — pipeline runs, watermarks, quality results — lives in Delta
tables rather than logs, so it can be queried and compared over time. This
module is the single place that reads and writes them.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession


def _now() -> datetime:
    return datetime.now(timezone.utc)


def create_platform_tables(spark: SparkSession, catalog: str) -> None:
    """Create the control schema + tables if absent (run once per environment)."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.platform")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.platform.pipeline_runs (
            run_id STRING, pipeline_name STRING, source_name STRING,
            started_at TIMESTAMP, completed_at TIMESTAMP, status STRING,
            records_read BIGINT, records_written BIGINT, records_rejected BIGINT,
            error_message STRING)
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.platform.ingestion_state (
            source_name STRING, watermark_column STRING,
            watermark_value STRING, updated_at TIMESTAMP)
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.platform.data_quality_results (
            run_id STRING, table_name STRING, check_name STRING, status STRING,
            metric DOUBLE, threshold DOUBLE, checked_at TIMESTAMP)
    """)


def start_run(spark: SparkSession, catalog: str, *, pipeline_name: str, source_name: str) -> str:
    """Insert a 'running' row and return its run_id."""
    run_id = str(uuid.uuid4())
    spark.createDataFrame([Row(
        run_id=run_id, pipeline_name=pipeline_name, source_name=source_name,
        started_at=_now(), completed_at=None, status="running",
        records_read=None, records_written=None, records_rejected=None,
        error_message=None,
    )]).write.mode("append").saveAsTable(f"{catalog}.platform.pipeline_runs")
    return run_id


def finish_run(spark: SparkSession, catalog: str, run_id: str, *, status: str,
               read: int = 0, written: int = 0, rejected: int = 0, error: str | None = None) -> None:
    """Close out a run row with final status and counts."""
    err = "NULL" if error is None else "'" + error.replace("'", "''") + "'"
    spark.sql(f"""
        MERGE INTO {catalog}.platform.pipeline_runs t
        USING (SELECT '{run_id}' AS run_id) s ON t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET
            completed_at = current_timestamp(), status = '{status}',
            records_read = {read}, records_written = {written},
            records_rejected = {rejected}, error_message = {err}
    """)


def get_watermark(spark: SparkSession, catalog: str, source_name: str,
                  default: str = "1970-01-01T00:00:00") -> str:
    """Latest successfully processed watermark for a source (for incremental reads)."""
    rows = (spark.table(f"{catalog}.platform.ingestion_state")
            .where(f"source_name = '{source_name}'").collect())
    return rows[0]["watermark_value"] if rows else default


def set_watermark(spark: SparkSession, catalog: str, source_name: str,
                  column: str, value: str) -> None:
    """Upsert the watermark so the next run only reads newer data."""
    spark.sql(f"""
        MERGE INTO {catalog}.platform.ingestion_state t
        USING (SELECT '{source_name}' AS source_name) s
          ON t.source_name = s.source_name
        WHEN MATCHED THEN UPDATE SET
            watermark_column = '{column}', watermark_value = '{value}',
            updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT
            (source_name, watermark_column, watermark_value, updated_at)
            VALUES ('{source_name}', '{column}', '{value}', current_timestamp())
    """)
