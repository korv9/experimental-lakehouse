# Databricks notebook source
"""Official Gutenberg catalog -> governed Volume snapshot -> Bronze."""
from __future__ import annotations

import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

REPOSITORY_ROOT = (
    Path(__file__).resolve().parents[3] if "__file__" in globals() else Path.cwd()
)
for import_root in (REPOSITORY_ROOT / "src", REPOSITORY_ROOT):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from lakehouse_platform.ingestion.catalog_files import (
    GzipCsvSnapshot,
    GzipCsvSource,
    land_gzip_csv_snapshot,
)
from lakehouse_platform.jobs import JobContext, process_job
from lakehouse_platform.metadata.control_tables import set_watermark
from products.philosophy_litterature.tables.bronze.gutenberg_catalog_raw.contract import (
    TableDefinition,
)

SOURCE_CONFIG = REPOSITORY_ROOT / "config" / "sources" / "gutenberg_catalog.yaml"


def build_gutenberg_catalog_raw(
    snapshot: GzipCsvSnapshot,
    context: JobContext,
) -> DataFrame:
    """Preserve every source row as JSON with immutable file lineage."""
    source_schema = T.StructType(
        [T.StructField(column, T.StringType(), True) for column in snapshot.header]
    )
    source = (
        context.spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .schema(source_schema)
        .csv(str(snapshot.path))
    )
    raw_payload = F.to_json(
        F.struct(
            *[F.col(f"`{column}`").alias(column) for column in snapshot.header]
        ),
        {"ignoreNullFields": "false"},
    )
    source_rows = source.select(
        F.col("`Text#`").alias("source_record_id"),
        raw_payload.alias("raw_payload"),
    ).filter(
        F.col("source_record_id").isNotNull()
        & (F.length(F.trim(F.col("source_record_id"))) > 0)
    )

    return source_rows.select(
        F.sha2(
            F.concat_ws(
                "\u0000",
                F.lit(snapshot.source.name),
                F.lit(snapshot.checksum),
                F.col("source_record_id"),
                F.col("raw_payload"),
            ),
            256,
        ).alias("ingestion_id"),
        F.lit(snapshot.source.name).alias("source_name"),
        F.lit(snapshot.source.url).alias("source_url"),
        F.lit(str(snapshot.path)).alias("source_file"),
        F.lit(snapshot.checksum).alias("source_checksum"),
        F.lit(snapshot.source_modified_at).cast("string").alias("source_modified_at"),
        F.lit(snapshot.snapshot_date).cast("date").alias("source_snapshot_date"),
        F.lit(datetime.now(timezone.utc)).cast("timestamp").alias("ingested_at"),
        F.lit(context.run_id).alias("run_id"),
        F.col("source_record_id"),
        F.col("raw_payload"),
        F.lit(snapshot.source.schema_version).alias("schema_version"),
    ).dropDuplicates(["ingestion_id"])


def validate_gutenberg_catalog_raw(result: DataFrame) -> None:
    duplicate = (
        result.groupBy("source_record_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
    )
    if duplicate.count():
        raise ValueError("Gutenberg catalog contains conflicting duplicate Text# rows")


def _parameter(name: str, default: str) -> str:
    dbutils_object: Any = globals().get("dbutils")
    if dbutils_object is None:
        return os.environ.get(name.upper(), default)
    dbutils_object.widgets.text(name, default)
    return str(dbutils_object.widgets.get(name))


def main(spark_session: Any | None = None) -> str:
    if spark_session is None:
        from pyspark.sql import SparkSession

        spark_session = SparkSession.getActiveSession()
        if spark_session is None:
            raise RuntimeError("Attach Unity Catalog-enabled compute")

    catalog = _parameter("catalog", "dev_lakehouse")
    snapshot_value = _parameter("snapshot_date", "").strip()
    snapshot_date = (
        date.fromisoformat(snapshot_value)
        if snapshot_value
        else datetime.now(timezone.utc).date()
    )
    source_config = GzipCsvSource.from_yaml(SOURCE_CONFIG)

    def build_table(context: JobContext) -> DataFrame:
        snapshot = land_gzip_csv_snapshot(
            context.spark,
            catalog=context.catalog,
            source=source_config,
            snapshot_date=snapshot_date,
            run_id=context.run_id,
        )
        return build_gutenberg_catalog_raw(snapshot, context)

    def update_watermark(context: JobContext) -> None:
        set_watermark(
            context.spark,
            context.catalog,
            source_config.name,
            "source_snapshot_date",
            snapshot_date.isoformat(),
        )

    print("=" * 88)
    print("GUTENBERG CATALOG TO BRONZE")
    print(f"Source:   {source_config.url}")
    print(f"Snapshot: {snapshot_date}")
    print(f"Target:   {catalog}.{TableDefinition.object_location()}")

    job_config = {
        "pipeline_name": "ingest_gutenberg_catalog",
        "source_name": source_config.name,
        "target": {
            "path": TableDefinition.object_location(),
            "format": "delta",
            "mode": "merge",
            "keys": ["ingestion_id"],
            "when_matched": "ignore",
        },
        "transformation": build_table,
        "validation": {
            "contract": TableDefinition,
            "checks": [validate_gutenberg_catalog_raw],
        },
        "on_success": update_watermark,
    }

    return process_job(spark_session, job_config, catalog=catalog)


if __name__ == "__main__":
    main()
