"""Select reviewed Philosophy corpus works from normalized Gutenberg metadata."""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse_platform.io.writers import write_output
from lakehouse_platform.metadata.control_tables import finish_run, start_run
from lakehouse_platform.metadata.unity_catalog import UnityCatalogLayout
from lakehouse_platform.observability.progress import progress
from products.philosophy_litterature.selection import load_selection
from products.philosophy_litterature.tables.silver.philosophy_litterature_work.contract import (
    TABLE,
    TableDefinition,
)

SELECTION_SCHEMA = T.StructType(
    [
        T.StructField("corpus_id", T.StringType(), False),
        T.StructField("corpus_work_id", T.StringType(), False),
        T.StructField("gutenberg_id", T.StringType(), False),
        T.StructField("period", T.StringType(), False),
        T.StructField("canonical_author", T.StringType(), False),
        T.StructField("canonical_title", T.StringType(), False),
        T.StructField("match_status", T.StringType(), False),
        T.StructField("text_url", T.StringType(), True),
    ]
)


def transform(catalog_works: DataFrame, selection: DataFrame) -> DataFrame:
    progress("PHILOSOPHY", "Joining approved corpus intent to official metadata")
    catalog = catalog_works.alias("catalog")
    approved = selection.alias("selection")
    return approved.join(catalog, "gutenberg_id", "inner").select(
        F.col("selection.corpus_id"),
        F.col("selection.corpus_work_id"),
        F.col("gutenberg_id"),
        F.col("selection.period"),
        F.col("selection.canonical_author"),
        F.col("selection.canonical_title"),
        F.col("selection.match_status"),
        F.col("catalog.title"),
        F.col("catalog.language_codes"),
        F.col("catalog.authors"),
        F.col("catalog.subjects"),
        F.col("catalog.locc_classes"),
        F.col("catalog.bookshelves"),
        F.col("selection.text_url"),
        F.col("catalog.landing_page_url"),
        F.col("catalog.source_snapshot_date"),
        F.col("catalog.source_checksum"),
        F.col("catalog.ingested_at"),
    )


def run(
    spark: SparkSession,
    report_path: str | Path,
    *,
    catalog: str = "dev_lakehouse",
) -> str:
    UnityCatalogLayout(catalog)
    run_id = start_run(
        spark,
        catalog,
        pipeline_name="select_philosophy_corpus",
        source_name="project_gutenberg_catalog",
    )
    target = f"{catalog}.{TABLE}"
    try:
        selection_rows = load_selection(report_path)
        selection = spark.createDataFrame(selection_rows, schema=SELECTION_SCHEMA)
        source = spark.table(f"{catalog}.silver.gutenberg_work")
        result = transform(source, selection)
        selected = len(selection_rows)
        rows = result.count()
        if rows != selected:
            found = {row["corpus_work_id"] for row in result.select("corpus_work_id").collect()}
            missing = sorted(row["corpus_work_id"] for row in selection_rows if row["corpus_work_id"] not in found)
            raise RuntimeError(
                f"Official catalog is missing {len(missing)} approved corpus works: {missing}"
            )
        non_english = result.where(
            ~F.array_contains(F.col("language_codes"), "en")
        ).count()
        if non_english:
            raise RuntimeError(
                f"Approved English-language corpus contains {non_english} non-English rows"
            )
        TableDefinition.validate(result)
        progress("PHILOSOPHY", "Merging approved corpus into Silver", rows=rows)
        write_output(
            spark,
            result,
            "delta_merge",
            {"table": target, "keys": ["corpus_work_id"], "format": "delta"},
        )
        finish_run(spark, catalog, run_id, status="success", read=source.count(), written=rows)
        progress("PHILOSOPHY", "Approved corpus completed", table=target, rows=rows)
        return run_id
    except Exception as error:
        finish_run(spark, catalog, run_id, status="failed", error=str(error))
        progress("PHILOSOPHY", "Approved corpus selection failed", error=str(error))
        raise
