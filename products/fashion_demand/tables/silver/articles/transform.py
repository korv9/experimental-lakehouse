"""Bronze -> Silver: the descriptive attributes of an article.

Only the columns a demand model or its segmentation actually uses are promoted.
The other ~15 stay in Bronze rather than being carried forward "just in case":
a Silver table nobody can explain is how a lakehouse starts rotting, and the raw
payload is still there for whoever needs department-level detail later.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Source column -> Silver column. Promoting a new attribute means adding a line
# here and a field to the contract; the test that compares the two catches
# either one being forgotten.
PROMOTED = {
    "article_id": "article_id",
    "prod_name": "product_name",
    "product_type_name": "product_type",
    "product_group_name": "product_group",
    "colour_group_name": "colour_group",
    "department_name": "department",
    "index_group_name": "index_group",
    "section_name": "section",
    "garment_group_name": "garment_group",
}


def _parse(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    return df.select(
        *[
            F.trim(F.get_json_object("raw_payload", f"$['{source}']")).alias(target)
            for source, target in PROMOTED.items()
        ],
        F.col("ingested_at"),
    )


def _deduplicate(df: DataFrame) -> DataFrame:
    """Keep one row per article: the most recently landed.

    The export is a snapshot, so duplicates only appear when the same file is
    landed twice. Collapsing them here means the Gold dimension has a real
    primary key regardless of how many times Bronze was replayed.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    latest = Window.partitionBy("article_id").orderBy(F.col("ingested_at").desc())
    return (
        df.withColumn("_rank", F.row_number().over(latest))
        .where(F.col("_rank") == 1)
        .drop("_rank")
    )


def transform(df: DataFrame, options: dict | None = None) -> DataFrame:
    """Raw article payloads -> one typed row per article."""
    return _deduplicate(_parse(df))
