from pyspark.sql import DataFrame

from lakehouse_platform.observability.progress import progress
from lakehouse_platform.transforms.hashing import internal_id_hash


def build(source: DataFrame, options: dict | None = None) -> DataFrame:
    progress("DIM_WORK", "Building dimension", grain="one row per work")
    return (
        source.select(
            internal_id_hash("work_id").alias("work_key"),
            "work_id",
            "title",
            "language",
            "year",
        )
        .dropDuplicates(["work_key"])
    )
