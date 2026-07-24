from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_platform.observability.progress import progress
from lakehouse_platform.transforms.hashing import internal_id_hash


def build(source: DataFrame, options: dict | None = None) -> DataFrame:
    progress("DIM_CATEGORY", "Building dimension", grain="one row per category")
    return (
        source.where(F.col("category").isNotNull())
        .select(
            internal_id_hash("category").alias("category_key"),
            F.col("category").alias("category_name"),
        )
        .dropDuplicates(["category_key"])
    )
