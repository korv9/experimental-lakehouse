"""Official Gutenberg catalog feed -> governed file -> source Bronze."""
from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse_platform.ingestion.catalog_files import (
    validate_gzip_csv,
    write_artifact_manifest,
)
from lakehouse_platform.ingestion.files import download_file
from lakehouse_platform.io.writers import write_output
from lakehouse_platform.metadata.control_tables import (
    finish_run,
    record_download,
    set_watermark,
    start_run,
)
from lakehouse_platform.metadata.unity_catalog import UnityCatalogLayout
from lakehouse_platform.observability.progress import progress
from products.philosophy_litterature.tables.bronze.gutenberg_catalog_raw.contract import (
    TableDefinition,
)


def load_config(path: str | Path) -> dict[str, Any]:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def build_bronze(
    spark: SparkSession,
    path: str | Path,
    *,
    header: list[str],
    source_name: str,
    source_url: str,
    source_checksum: str,
    source_modified_at: str | None,
    snapshot_date: date,
    run_id: str,
    schema_version: str,
) -> DataFrame:
    source_schema = T.StructType(
        [T.StructField(column, T.StringType(), True) for column in header]
    )
    source = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .schema(source_schema)
        .csv(str(path))
    )
    payload = F.to_json(
        F.struct(*[F.col(f"`{column}`").alias(column) for column in header]),
        {"ignoreNullFields": "false"},
    )
    staged = source.select(
        F.col("`Text#`").alias("source_record_id"),
        payload.alias("raw_payload"),
    ).where(
        F.col("source_record_id").isNotNull()
        & (F.length(F.trim(F.col("source_record_id"))) > 0)
    )
    return staged.select(
        F.sha2(
            F.concat(
                F.lit(source_name),
                F.lit("\u0000"),
                F.lit(source_checksum),
                F.lit("\u0000"),
                F.col("source_record_id"),
                F.lit("\u0000"),
                F.col("raw_payload"),
            ),
            256,
        ).alias("ingestion_id"),
        F.lit(source_name).alias("source_name"),
        F.lit(source_url).alias("source_url"),
        F.lit(str(path)).alias("source_file"),
        F.lit(source_checksum).alias("source_checksum"),
        F.lit(source_modified_at).cast("string").alias("source_modified_at"),
        F.lit(snapshot_date).cast("date").alias("source_snapshot_date"),
        F.lit(datetime.now(timezone.utc)).cast("timestamp").alias("ingested_at"),
        F.lit(run_id).alias("run_id"),
        "source_record_id",
        "raw_payload",
        F.lit(schema_version).alias("schema_version"),
    )


def run(
    spark: SparkSession,
    config_path: str | Path,
    *,
    catalog: str = "dev_lakehouse",
    snapshot_date: date | None = None,
) -> str:
    """Land, validate and merge one official catalog snapshot into Bronze."""
    config = load_config(config_path)
    source_name = str(config["source_name"])
    source_url = str(config["url"])
    snapshot_date = snapshot_date or datetime.now(timezone.utc).date()
    file_config = config["file"]
    layout = UnityCatalogLayout(catalog)
    target_path = Path(
        layout.source_path(
            str(file_config["landing_source"]),
            str(file_config["landing_subpath"]),
            snapshot_date.isoformat(),
            str(file_config["name"]),
        )
    )
    target_table = f"{catalog}.{config['destination']['bronze_table']}"
    run_id = start_run(
        spark,
        catalog,
        pipeline_name="ingest_gutenberg_catalog",
        source_name=source_name,
    )
    progress(
        "GUTENBERG",
        "Catalog ingestion started",
        run_id=run_id,
        snapshot=snapshot_date,
        target=target_table,
    )
    try:
        request = config.get("request", {})
        result = download_file(
            source_url,
            target_path,
            headers={str(k): str(v) for k, v in request.get("headers", {}).items()},
            timeout=float(request.get("timeout_seconds", 120)),
            max_retries=int(request.get("max_retries", 4)),
        )
        header = validate_gzip_csv(target_path, list(file_config["required_columns"]))
        manifest_path = write_artifact_manifest(
            result,
            source_name=source_name,
            source_url=source_url,
            snapshot_date=snapshot_date,
        )
        record_download(
            spark,
            catalog,
            source_name=source_name,
            source_record_id=snapshot_date.isoformat(),
            source_url=source_url,
            volume_path=str(target_path),
            sha256=result.sha256,
            size_bytes=result.size_bytes,
            source_etag=result.source_etag,
            status="downloaded" if result.downloaded else "reused",
            run_id=run_id,
        )
        bronze = build_bronze(
            spark,
            target_path,
            header=header,
            source_name=source_name,
            source_url=source_url,
            source_checksum=result.sha256,
            source_modified_at=result.source_last_modified,
            snapshot_date=snapshot_date,
            run_id=run_id,
            schema_version=str(config.get("schema_version", "v1")),
        ).dropDuplicates(["ingestion_id"])
        TableDefinition.validate(bronze)
        rows = bronze.count()
        distinct_records = bronze.select("source_record_id").distinct().count()
        if distinct_records != rows:
            raise RuntimeError(
                "Catalog snapshot contains conflicting duplicate Text# rows: "
                f"rows={rows}, distinct_ids={distinct_records}"
            )
        progress("GUTENBERG", "Merging catalog rows into Bronze", rows=rows)
        write_output(
            spark,
            bronze,
            "delta_merge",
            {
                "table": target_table,
                "keys": ["ingestion_id"],
                "format": "delta",
                "when_matched": "ignore",
            },
        )
        set_watermark(
            spark,
            catalog,
            source_name,
            "source_snapshot_date",
            snapshot_date.isoformat(),
        )
        finish_run(spark, catalog, run_id, status="success", read=rows, written=rows)
        progress(
            "GUTENBERG",
            "Catalog ingestion completed",
            rows=rows,
            manifest=manifest_path,
        )
        return run_id
    except Exception as error:
        finish_run(spark, catalog, run_id, status="failed", error=str(error))
        progress("GUTENBERG", "Catalog ingestion failed", error=str(error))
        raise
