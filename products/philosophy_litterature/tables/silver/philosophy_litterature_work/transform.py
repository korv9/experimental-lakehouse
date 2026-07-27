"""Silver Gutenberg works + reviewed selection -> the Philosophy corpus.

A multi-input ACON transformation: the frames arrive positionally in the order
the ACON declares them under ``input_ids``. Keeping the join in the graph rather
than hiding it inside one transformation means both sides are visible as inputs.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_platform.observability.progress import progress


def transform(
    catalog_works: DataFrame,
    selection: DataFrame,
    options: dict | None = None,
) -> DataFrame:
    progress("PHILOSOPHY", "Joining reviewed selection to catalog works")
    return (
        selection.alias("selection")
        .join(catalog_works.alias("catalog"), "gutenberg_id", "inner")
        .select(
            F.col("selection.corpus_id"),
            F.col("selection.corpus_work_id"),
            F.col("gutenberg_id"),
            F.col("selection.period"),
            F.col("selection.canonical_author"),
            F.col("selection.canonical_title"),
            F.col("selection.match_status"),
            F.col("catalog.title"),
            F.col("catalog.language_codes"),
            F.col("catalog.authors"),
            F.col("catalog.subjects"),
            F.col("catalog.locc_classes"),
            F.col("catalog.bookshelves"),
            F.col("selection.text_url"),
            F.col("catalog.landing_page_url"),
            F.col("catalog.source_snapshot_date"),
            F.col("catalog.source_checksum"),
            F.col("catalog.ingested_at"),
        )
    )
