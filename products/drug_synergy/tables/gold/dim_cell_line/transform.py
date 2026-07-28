"""Cell line dimension, built from the cell lines DepMap knows about."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_platform.observability.progress import progress
from lakehouse_platform.transforms.hashing import internal_id_hash


def build(cell_line: DataFrame, options: dict | None = None) -> DataFrame:
    progress("DIM_CELL_LINE", "Building dimension", grain="one row per cell line")
    return (
        cell_line.select(
            internal_id_hash("cell_line_key").alias("cell_line_key"),
            F.col("cell_line_key").alias("cell_line_id"),
            "model_id",
            "cell_line_name",
            "oncotree_lineage",
        )
        .dropDuplicates(["cell_line_key"])
    )
