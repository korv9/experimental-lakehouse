from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_platform.observability.progress import progress
from lakehouse_platform.transforms.hashing import internal_id_hash


def build(source: DataFrame, options: dict | None = None) -> DataFrame:
    progress("FACT_WORK", "Building fact", grain="one row per current work")
    return source.select(
        internal_id_hash("work_id").alias("work_key"),
        internal_id_hash("author_id").alias("author_key"),
        internal_id_hash("category").alias("category_key"),
        F.date_format(F.to_date("updated_at"), "yyyyMMdd").cast("int").alias("date_key"),
        F.lit(1).cast("long").alias("work_count"),
        F.size(F.coalesce(F.col("tags"), F.array().cast("array<string>"))).alias("tag_count"),
    )
