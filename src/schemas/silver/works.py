"""Silver schema for ``silver.works`` — the enforced shape of a cleaned record.

Schema *enforcement* happens when we parse the JSON in ``raw_payload`` against
``RAW_WORK``: missing fields become null, unexpected fields are dropped, and
types are coerced. Downstream code can then rely on these columns existing with
these types.
"""
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# Shape we expect inside bronze.raw_payload (nested author is flattened later):
RAW_WORK = StructType([
    StructField("id", StringType()),
    StructField("title", StringType()),
    StructField("author", StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
    ])),
    StructField("category", StringType()),
    StructField("year", IntegerType()),
    StructField("language", StringType()),
    StructField("updated_at", StringType()),
])

TABLE = "silver.works"
BUSINESS_KEY = "work_id"   # natural key used for dedup + MERGE
