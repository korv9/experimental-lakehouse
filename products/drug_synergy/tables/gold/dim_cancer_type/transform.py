"""Cancer type dimension, derived from the DepMap lineage annotation."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_platform.observability.progress import progress
from lakehouse_platform.transforms.hashing import internal_id_hash

UNKNOWN = "Unknown"


def build(cell_line: DataFrame, options: dict | None = None) -> DataFrame:
    progress("DIM_CANCER_TYPE", "Building dimension", grain="one row per lineage")
    # a missing lineage becomes an explicit member, so facts never lose their FK
    labelled = cell_line.withColumn(
        "oncotree_lineage", F.coalesce(F.col("oncotree_lineage"), F.lit(UNKNOWN))
    )
    return (
        labelled.groupBy("oncotree_lineage")
        .agg(F.countDistinct("cell_line_key").cast("int").alias("n_cell_lines"))
        .select(
            internal_id_hash("oncotree_lineage").alias("cancer_type_key"),
            "oncotree_lineage",
            "n_cell_lines",
        )
    )
