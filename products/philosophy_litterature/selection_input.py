"""The reviewed corpus selection, exposed as an ACON input.

The selection is neither a table nor a plain file read: it is a curated report
that ``selection.load_selection`` filters down to approved works. Wrapping it as
a ``product_callable`` reader keeps it visible in the ACON graph instead of
hiding it inside a transformation.
"""
from __future__ import annotations

from pyspark.sql import types as T

from products.philosophy_litterature.selection import load_selection

SELECTION_SCHEMA = T.StructType(
    [
        T.StructField("corpus_id", T.StringType(), False),
        T.StructField("corpus_work_id", T.StringType(), False),
        T.StructField("gutenberg_id", T.StringType(), False),
        T.StructField("period", T.StringType(), False),
        T.StructField("canonical_author", T.StringType(), False),
        T.StructField("canonical_title", T.StringType(), False),
        T.StructField("match_status", T.StringType(), False),
        T.StructField("text_url", T.StringType(), True),
    ]
)


def read(spark, options: dict):
    """ACON reader entry point: ``options['report']`` -> selection DataFrame."""
    rows = load_selection(options["report"])
    return spark.createDataFrame(rows, schema=SELECTION_SCHEMA)
