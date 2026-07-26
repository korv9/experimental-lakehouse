"""Normalize source Bronze catalog payloads into one current row per work."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse_platform.io.writers import write_output
from lakehouse_platform.metadata.control_tables import finish_run, start_run
from lakehouse_platform.metadata.unity_catalog import UnityCatalogLayout
from lakehouse_platform.observability.progress import progress
from products.philosophy_litterature.tables.silver.gutenberg_work.contract import (
    TABLE,
    TableDefinition,
)

RAW_CATALOG = T.StructType(
    [
        T.StructField("Text#", T.StringType()),
        T.StructField("Type", T.StringType()),
        T.StructField("Issued", T.StringType()),
        T.StructField("Title", T.StringType()),
        T.StructField("Language", T.StringType()),
        T.StructField("Authors", T.StringType()),
        T.StructField("Subjects", T.StringType()),
        T.StructField("LoCC", T.StringType()),
        T.StructField("Bookshelves", T.StringType()),
    ]
)


def _string_array(column: F.Column) -> F.Column:
    values = F.transform(F.split(F.coalesce(column, F.lit("")), ";"), lambda item: F.trim(item))
    return F.filter(values, lambda item: F.length(item) > 0)


def transform(bronze: DataFrame, options: dict | None = None) -> DataFrame:
    progress("GUTENBERG", "Parsing official catalog Bronze payloads")
    parsed = bronze.withColumn("record", F.from_json("raw_payload", RAW_CATALOG))
    latest = Window.partitionBy(F.col("record.`Text#`")).orderBy(
        F.col("source_snapshot_date").desc(),
        F.col("ingested_at").desc(),
    )
    result = (
        parsed.withColumn("_row_number", F.row_number().over(latest))
        .where(F.col("_row_number") == 1)
        .select(
            F.col("record.`Text#`").alias("gutenberg_id"),
            F.col("record.Type").alias("media_type"),
            F.to_date(F.col("record.Issued")).alias("issued_date"),
            F.col("record.Title").alias("title"),
            _string_array(F.col("record.Language")).alias("language_codes"),
            _string_array(F.col("record.Authors")).alias("authors"),
            _string_array(F.col("record.Subjects")).alias("subjects"),
            _string_array(F.col("record.LoCC")).alias("locc_classes"),
            _string_array(F.col("record.Bookshelves")).alias("bookshelves"),
            F.concat(F.lit("https://www.gutenberg.org/ebooks/"), F.col("record.`Text#`"))
            .alias("landing_page_url"),
            "source_snapshot_date",
            "source_checksum",
            "source_file",
            "ingested_at",
        )
    )
    progress("GUTENBERG", "Normalized Gutenberg work graph created")
    return result


def run(spark: SparkSession, catalog: str = "dev_lakehouse") -> str:
    UnityCatalogLayout(catalog)
    run_id = start_run(
        spark,
        catalog,
        pipeline_name="gutenberg_catalog_bronze_to_silver",
        source_name="project_gutenberg_catalog",
    )
    target = f"{catalog}.{TABLE}"
    try:
        source = spark.table(f"{catalog}.bronze.gutenberg_catalog_raw")
        silver = transform(source)
        TableDefinition.validate(silver)
        rows = silver.count()
        missing_titles = silver.where(F.col("title").isNull() | (F.length(F.trim("title")) == 0)).count()
        if missing_titles:
            raise RuntimeError(f"Normalized catalog contains {missing_titles} works without titles")
        progress("GUTENBERG", "Merging normalized catalog into Silver", rows=rows)
        write_output(
            spark,
            silver,
            "delta_merge",
            {"table": target, "keys": ["gutenberg_id"], "format": "delta"},
        )
        finish_run(spark, catalog, run_id, status="success", read=rows, written=rows)
        progress("GUTENBERG", "Silver catalog completed", table=target, rows=rows)
        return run_id
    except Exception as error:
        finish_run(spark, catalog, run_id, status="failed", error=str(error))
        progress("GUTENBERG", "Silver catalog failed", error=str(error))
        raise
