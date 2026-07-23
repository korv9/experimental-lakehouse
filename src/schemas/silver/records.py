"""Silver schema for ``silver.records`` — the structured target of the messy demo.

This StructType IS the contract. The cleaning UDF must return exactly these
fields with these types, which is how a wildly heterogeneous raw feed becomes a
predictable table. Note the range of types: strings, arrays, ints, doubles,
booleans, and geo coordinates.
"""
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Output of transformations.cleaning.clean_record (order/types must match):
CLEAN_RECORD = StructType([
    StructField("record_id", StringType()),
    StructField("title", StringType()),
    StructField("creators", ArrayType(StringType())),
    StructField("summary", StringType()),
    StructField("category", StringType()),
    StructField("labels", ArrayType(StringType())),
    StructField("year", IntegerType()),
    StructField("rating", DoubleType()),
    StructField("is_public", BooleanType()),
    StructField("price", DoubleType()),
    StructField("email", StringType()),
    StructField("url", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("language", StringType()),
    StructField("updated_at", StringType()),   # ISO date string
])

TABLE = "silver.records"
BUSINESS_KEY = "record_id"
