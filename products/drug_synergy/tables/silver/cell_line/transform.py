"""Bronze -> Silver for DepMap cell lines.

Ports the cell-line matching half of the original ``Omic_ny.py``: normalise the
name so DrugComb's ``A-549`` and DepMap's ``A549`` become the same key, and keep
only the newest release per cell line.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse_platform.observability.progress import progress
from products.drug_synergy.normalisation import normalise_cell_line

SOURCE_SCHEMA = T.StructType([
    T.StructField("ModelID", T.StringType()),
    T.StructField("CellLineName", T.StringType()),
    T.StructField("StrippedCellLineName", T.StringType()),
    T.StructField("OncotreeLineage", T.StringType()),
])


def transform(bronze: DataFrame, options: dict | None = None) -> DataFrame:
    progress("DRUG_SYNERGY", "Normalising DepMap cell lines")
    parsed = bronze.withColumn("p", F.from_json("raw_payload", SOURCE_SCHEMA)).select(
        F.col("p.ModelID").alias("model_id"),
        F.col("p.CellLineName").alias("cell_line_name"),
        F.col("p.StrippedCellLineName").alias("stripped_cell_line_name"),
        F.col("p.OncotreeLineage").alias("oncotree_lineage"),
        F.col("schema_version").alias("depmap_release"),
        F.col("ingested_at"),
    )

    # prefer StrippedCellLineName, fall back to the display name
    keyed = parsed.withColumn(
        "cell_line_key",
        F.coalesce(
            normalise_cell_line(F.col("stripped_cell_line_name")),
            normalise_cell_line(F.col("cell_line_name")),
        ),
    )

    # newest release wins, so a re-download supersedes rather than duplicates
    newest = Window.partitionBy("cell_line_key").orderBy(F.col("ingested_at").desc())
    return (
        keyed.withColumn("_rn", F.row_number().over(newest))
        .where("_rn = 1")
        .drop("_rn")
        .select(
            "cell_line_key", "model_id", "cell_line_name", "stripped_cell_line_name",
            "oncotree_lineage", "depmap_release", "ingested_at",
        )
    )
