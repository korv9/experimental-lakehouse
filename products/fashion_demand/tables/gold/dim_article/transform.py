"""Silver -> Gold: the article dimension the demand fact hangs off.

A dimension in a Kimball model is where descriptive text lives, so that the fact
can stay narrow: keys and additive measures only. Everything a planner would
slice demand by — product type, department, colour — belongs here, once, rather
than repeated on every one of the fact's tens of millions of rows.

The surrogate key is deterministic rather than a monotonically increasing id, so
a rebuild produces the same keys and the fact does not have to be rewritten.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def transform(df: DataFrame, options: dict | None = None) -> DataFrame:
    from pyspark.sql import functions as F

    from lakehouse_platform.transforms.hashing import internal_id_hash

    unknown = F.lit("Unknown")
    return df.select(
        internal_id_hash("article_id").alias("article_key"),
        F.col("article_id"),
        F.coalesce(F.col("product_name"), unknown).alias("product_name"),
        F.coalesce(F.col("product_type"), unknown).alias("product_type"),
        F.coalesce(F.col("product_group"), unknown).alias("product_group"),
        F.coalesce(F.col("colour_group"), unknown).alias("colour_group"),
        F.coalesce(F.col("department"), unknown).alias("department"),
        F.coalesce(F.col("index_group"), unknown).alias("index_group"),
        F.coalesce(F.col("section"), unknown).alias("section"),
        F.coalesce(F.col("garment_group"), unknown).alias("garment_group"),
        F.current_timestamp().alias("loaded_at"),
    )
