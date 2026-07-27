"""Bronze -> Silver for the Gutenberg catalog.

Domain logic only: parse the catalog fields out of the Bronze payload, keep the
newest snapshot per work, and project Silver names and types. Reading, the
contract check and the MERGE are declared in the product ACON.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lakehouse_platform.observability.progress import progress


def split_values(column):
    values = F.transform(
        F.split(F.coalesce(column, F.lit("")), ";"),
        lambda item: F.trim(item),
    )
    return F.filter(values, lambda item: F.length(item) > 0)


def transform(bronze: DataFrame, options: dict | None = None) -> DataFrame:
    """Normalize the latest version of every source-faithful Gutenberg row."""
    progress("PHILOSOPHY", "Normalizing Gutenberg catalog rows")

    # Step 1: extract the catalog fields stored in Bronze raw_payload.
    parsed = bronze.select(
        "*",
        F.json_tuple(
            "raw_payload",
            "Text#",
            "Type",
            "Issued",
            "Title",
            "Language",
            "Authors",
            "Subjects",
            "LoCC",
            "Bookshelves",
        ).alias(
            "raw_gutenberg_id",
            "raw_media_type",
            "raw_issued",
            "raw_title",
            "raw_languages",
            "raw_authors",
            "raw_subjects",
            "raw_locc",
            "raw_bookshelves",
        ),
    )

    # Step 2 and 3: keep only the current version of each catalog work.
    latest_snapshot = Window.partitionBy("raw_gutenberg_id").orderBy(
        F.col("source_snapshot_date").desc(),
        F.col("ingested_at").desc(),
    )
    latest = (
        parsed.select("*", F.row_number().over(latest_snapshot).alias("snapshot_rank"))
        .filter(F.col("snapshot_rank") == 1)
    )

    # Step 4: apply Silver names and types in one explicit projection.
    return latest.select(
        F.col("raw_gutenberg_id").alias("gutenberg_id"),
        F.col("raw_media_type").alias("media_type"),
        F.to_date("raw_issued").alias("issued_date"),
        F.trim("raw_title").alias("title"),
        split_values(F.col("raw_languages")).alias("language_codes"),
        split_values(F.col("raw_authors")).alias("authors"),
        split_values(F.col("raw_subjects")).alias("subjects"),
        split_values(F.col("raw_locc")).alias("locc_classes"),
        split_values(F.col("raw_bookshelves")).alias("bookshelves"),
        F.concat(
            F.lit("https://www.gutenberg.org/ebooks/"),
            F.col("raw_gutenberg_id"),
        ).alias("landing_page_url"),
        F.col("source_snapshot_date"),
        F.col("source_checksum"),
        F.col("source_file"),
        F.col("ingested_at"),
    )
