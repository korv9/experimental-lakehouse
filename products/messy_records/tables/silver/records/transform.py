"""Bronze -> Silver for the *messy* demo source.

Unlike the clean example, this raw feed is genuinely heterogeneous — a field can
be a string in one row and an object in the next — so a single ``from_json``
schema won't fit. Instead we parse each raw payload in Python and run it through
``cleaning.clean_record``, wrapped as one UDF that returns the fixed
``CLEAN_RECORD`` struct. That struct IS the schema enforcement: every row comes
out with the same typed columns regardless of how messy it went in.

Steps:
  1. read bronze raw_payload (JSON strings)
  2. clean_record UDF: json.loads + all cleaning helpers -> typed struct
  3. flatten struct to columns
  4. dedup: keep the latest row per record_id
(For fields with a *stable* type, native Spark functions are faster; the UDF is
the pragmatic tool when the raw shape varies row to row. Databricks' VARIANT type
is another option for semi-structured landing.)
"""
from __future__ import annotations

import json

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

from lakehouse_platform.observability.progress import progress
from lakehouse_platform.transforms.cleaning import clean_record
from products.messy_records.tables.silver.records.spark_schema import BUSINESS_KEY, CLEAN_RECORD


@udf(returnType=CLEAN_RECORD)
def _clean_udf(raw_payload: str):
    # one raw JSON string -> the typed, cleaned struct (same logic as local demo)
    return clean_record(json.loads(raw_payload))


def transform(bronze: DataFrame, options: dict | None = None) -> DataFrame:
    """Bronze rows -> cleaned, deduplicated silver rows (pure DataFrame in/out)."""
    progress("MESSY_RECORDS", "Cleaning Bronze records")
    cleaned = bronze.withColumn("c", _clean_udf(F.col("raw_payload")))
    flat = cleaned.select("c.*", F.col("ingested_at"))

    # latest row wins per business key (records repeat across ingestion batches)
    w = Window.partitionBy(BUSINESS_KEY).orderBy(F.col("ingested_at").desc())
    result = flat.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    progress("MESSY_RECORDS", "Silver transformation graph created")
    return result
