"""The fact: one row per canonical drug pair and cell line.

Multi-input: combinations and cell lines arrive in the order the ACON declares
them. The join to cell lines is a *left* join so a screen against a cell line
DepMap does not annotate is still counted; its cancer type falls back to the
explicit Unknown member of the dimension, keeping the foreign key valid.

Only keys and additive measures live here. Anything descriptive belongs in a
dimension, which is what lets the same numbers be sliced by drug, cell line or
tissue without rebuilding the fact.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_platform.observability.progress import progress
from lakehouse_platform.transforms.hashing import internal_id_hash
from products.drug_synergy.tables.gold.dim_cancer_type.transform import UNKNOWN


def build(
    combinations: DataFrame,
    cell_lines: DataFrame,
    options: dict | None = None,
) -> DataFrame:
    progress("FACT_DRUG_SYNERGY", "Building fact", grain="one row per pair and cell line")

    annotated = combinations.join(
        cell_lines.select("cell_line_key", "oncotree_lineage"),
        on="cell_line_key",
        how="left",
    ).withColumn(
        "oncotree_lineage", F.coalesce(F.col("oncotree_lineage"), F.lit(UNKNOWN))
    )

    return annotated.select(
        internal_id_hash("drug_min").alias("drug_min_key"),
        internal_id_hash("drug_max").alias("drug_max_key"),
        internal_id_hash("cell_line_key").alias("cell_line_key"),
        internal_id_hash("oncotree_lineage").alias("cancer_type_key"),
        "synergy_zip",
        "synergy_bliss",
        "synergy_loewe",
        "synergy_hsa",
        "is_synergistic",
        "n_measurements",
        F.lit(1).cast("long").alias("combination_count"),
    )
