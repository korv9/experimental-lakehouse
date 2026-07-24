"""Silver schema for ``silver.libris_works`` — Libris records, structured.

The enforced output of transformations.libris_parse.parse_libris_item. Note how
a rich, deeply-nested JSON-LD record collapses to a flat, typed row: creators and
subjects become string arrays, the year an int, language an ISO code.
"""
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

LIBRIS_WORK = StructType([
    StructField("record_id", StringType()),      # Libris system id (from meta.@id)
    StructField("title", StringType()),
    StructField("creators", ArrayType(StringType())),   # joined person names
    StructField("subjects", ArrayType(StringType())),   # subject prefLabels
    StructField("year", IntegerType()),
    StructField("language", StringType()),        # ISO 639 code, e.g. "swe"
    StructField("isbn", StringType()),
    StructField("publisher", StringType()),
    StructField("updated_at", StringType()),      # ISO date from meta.modified
])

TABLE = "silver.libris_works"
BUSINESS_KEY = "record_id"
