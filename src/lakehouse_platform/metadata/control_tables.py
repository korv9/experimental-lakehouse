"""Control tables (``platform.*``): metadata is data.

Operational state — pipeline runs, watermarks, quality results — lives in Delta
tables rather than logs, so it can be queried and compared over time. This
module is the single place that reads and writes them.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True)
class IngestionCheckpoint:
    pipeline_name: str
    source_name: str
    partition_key: str
    cursor: str | None
    watermark_value: str | None
    page_number: int
    status: str
    run_id: str | None


def _sql_string(value: str | None) -> str:
    return "NULL" if value is None else "'" + value.replace("'", "''") + "'"


def create_platform_tables(spark: SparkSession, catalog: str) -> None:
    """Create the control schema + tables if absent (run once per environment)."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.platform")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.platform.pipeline_runs (
            run_id STRING, pipeline_name STRING, source_name STRING,
            started_at TIMESTAMP, completed_at TIMESTAMP, status STRING,
            records_read BIGINT, records_written BIGINT, records_rejected BIGINT,
            error_message STRING)
        USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.platform.ingestion_state (
            source_name STRING, watermark_column STRING,
            watermark_value STRING, updated_at TIMESTAMP)
        USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.platform.data_quality_results (
            run_id STRING, table_name STRING, check_name STRING, status STRING,
            metric DOUBLE, threshold DOUBLE, checked_at TIMESTAMP)
        USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.platform.ingestion_checkpoints (
            pipeline_name STRING, source_name STRING, partition_key STRING,
            cursor STRING, watermark_value STRING, page_number BIGINT,
            status STRING, run_id STRING, updated_at TIMESTAMP)
        USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.platform.download_manifest (
            source_name STRING, source_record_id STRING, source_url STRING,
            volume_path STRING, sha256 STRING, size_bytes BIGINT,
            source_etag STRING, downloaded_at TIMESTAMP,
            status STRING, run_id STRING)
        USING DELTA
    """)


def start_run(spark: SparkSession, catalog: str, *, pipeline_name: str, source_name: str) -> str:
    """Insert a 'running' row and return its run_id."""
    run_id = str(uuid.uuid4())
    spark.sql(f"""
        INSERT INTO {catalog}.platform.pipeline_runs (
            run_id, pipeline_name, source_name, started_at, completed_at, status,
            records_read, records_written, records_rejected, error_message
        ) VALUES (
            {_sql_string(run_id)}, {_sql_string(pipeline_name)}, {_sql_string(source_name)},
            current_timestamp(), NULL, 'running', NULL, NULL, NULL, NULL
        )
    """)
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


def record_download(
    spark: SparkSession,
    catalog: str,
    *,
    source_name: str,
    source_record_id: str,
    source_url: str,
    volume_path: str,
    sha256: str,
    size_bytes: int,
    source_etag: str | None,
    status: str,
    run_id: str,
) -> None:
    """Append an immutable file-level lineage record to the download manifest."""
    values = {
        key: _sql_string(value)
        for key, value in {
            "source_name": source_name,
            "source_record_id": source_record_id,
            "source_url": source_url,
            "volume_path": volume_path,
            "sha256": sha256,
            "source_etag": source_etag,
            "status": status,
            "run_id": run_id,
        }.items()
    }
    spark.sql(f"""
        INSERT INTO {catalog}.platform.download_manifest (
            source_name, source_record_id, source_url, volume_path, sha256,
            size_bytes, source_etag, downloaded_at, status, run_id
        ) VALUES (
            {values["source_name"]}, {values["source_record_id"]},
            {values["source_url"]}, {values["volume_path"]}, {values["sha256"]},
            {int(size_bytes)}, {values["source_etag"]}, current_timestamp(),
            {values["status"]}, {values["run_id"]}
        )
    """)


def get_checkpoint(
    spark: SparkSession,
    catalog: str,
    *,
    pipeline_name: str,
    source_name: str,
    partition_key: str = "default",
) -> IngestionCheckpoint | None:
    """Return durable state for resuming one source partition."""
    rows = (
        spark.table(f"{catalog}.platform.ingestion_checkpoints")
        .where(
            (F.col("pipeline_name") == pipeline_name)
            & (F.col("source_name") == source_name)
            & (F.col("partition_key") == partition_key)
        )
        .orderBy(F.col("updated_at").desc())
        .limit(1)
        .collect()
    )
    if not rows:
        return None
    row = rows[0]
    return IngestionCheckpoint(
        pipeline_name=row["pipeline_name"],
        source_name=row["source_name"],
        partition_key=row["partition_key"],
        cursor=row["cursor"],
        watermark_value=row["watermark_value"],
        page_number=row["page_number"],
        status=row["status"],
        run_id=row["run_id"],
    )


def set_checkpoint(
    spark: SparkSession,
    catalog: str,
    checkpoint: IngestionCheckpoint,
) -> None:
    """Atomically upsert a cursor/watermark only after a page is committed."""
    values = {
        key: _sql_string(value)
        for key, value in {
            "pipeline": checkpoint.pipeline_name,
            "source": checkpoint.source_name,
            "partition": checkpoint.partition_key,
            "cursor": checkpoint.cursor,
            "watermark": checkpoint.watermark_value,
            "status": checkpoint.status,
            "run_id": checkpoint.run_id,
        }.items()
    }
    spark.sql(f"""
        MERGE INTO {catalog}.platform.ingestion_checkpoints t
        USING (
            SELECT {values["pipeline"]} AS pipeline_name,
                   {values["source"]} AS source_name,
                   {values["partition"]} AS partition_key
        ) s
        ON t.pipeline_name = s.pipeline_name
          AND t.source_name = s.source_name
          AND t.partition_key = s.partition_key
        WHEN MATCHED THEN UPDATE SET
            cursor = {values["cursor"]},
            watermark_value = {values["watermark"]},
            page_number = {int(checkpoint.page_number)},
            status = {values["status"]},
            run_id = {values["run_id"]},
            updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            pipeline_name, source_name, partition_key, cursor, watermark_value,
            page_number, status, run_id, updated_at
        ) VALUES (
            {values["pipeline"]}, {values["source"]}, {values["partition"]},
            {values["cursor"]}, {values["watermark"]},
            {int(checkpoint.page_number)}, {values["status"]},
            {values["run_id"]}, current_timestamp()
        )
    """)
