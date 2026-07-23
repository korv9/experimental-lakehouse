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

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

from src.schemas.silver.records import BUSINESS_KEY, CLEAN_RECORD, TABLE
from src.transformations.cleaning import clean_record


@udf(returnType=CLEAN_RECORD)
def _clean_udf(raw_payload: str):
    # one raw JSON string -> the typed, cleaned struct (same logic as local demo)
    return clean_record(json.loads(raw_payload))


def transform(bronze: DataFrame) -> DataFrame:
    """Bronze rows -> cleaned, deduplicated silver rows (pure DataFrame in/out)."""
    cleaned = bronze.withColumn("c", _clean_udf(F.col("raw_payload")))
    flat = cleaned.select("c.*", F.col("ingested_at"))

    # latest row wins per business key (records repeat across ingestion batches)
    w = Window.partitionBy(BUSINESS_KEY).orderBy(F.col("ingested_at").desc())
    return flat.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def run(spark: SparkSession, catalog: str = "dev_lakehouse", source: str = "messy_demo") -> int:
    bronze = spark.table(f"{catalog}.bronze.{source}_records")
    silver = transform(bronze)
    silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{TABLE}")
    return silver.count()
