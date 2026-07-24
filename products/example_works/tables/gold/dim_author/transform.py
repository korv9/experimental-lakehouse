from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_platform.observability.progress import progress
from lakehouse_platform.transforms.hashing import internal_id_hash


def build(source: DataFrame, options: dict | None = None) -> DataFrame:
    progress("DIM_AUTHOR", "Building dimension", grain="one row per author")
    return (
        source.where(F.col("author_id").isNotNull())
        .select(
            internal_id_hash("author_id").alias("author_key"),
            "author_id",
            "author_name",
        )
        .dropDuplicates(["author_key"])
    )
