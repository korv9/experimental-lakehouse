"""Bronze -> Silver for the Libris source.

Same pattern as the messy demo: each raw JSON-LD payload is parsed by a single
UDF (reusing the pure-Python ``parse_libris_item``) that returns the fixed
``LIBRIS_WORK`` struct — schema enforcement over deeply-nested linked data — then
dedup to the latest row per Libris id.
"""
from __future__ import annotations

import json

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

from src.schemas.silver.libris_works import BUSINESS_KEY, LIBRIS_WORK, TABLE
from src.transformations.libris_parse import parse_libris_item


@udf(returnType=LIBRIS_WORK)
def _parse_udf(raw_payload: str):
    return parse_libris_item(json.loads(raw_payload))


def transform(bronze: DataFrame) -> DataFrame:
    """Bronze rows -> cleaned, deduplicated silver rows (pure DataFrame in/out)."""
    parsed = bronze.withColumn("c", _parse_udf(F.col("raw_payload")))
    flat = parsed.select("c.*", F.col("ingested_at"))
    w = Window.partitionBy(BUSINESS_KEY).orderBy(F.col("ingested_at").desc())
    return flat.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def run(spark: SparkSession, catalog: str = "dev_lakehouse", source: str = "libris_find") -> int:
    bronze = spark.table(f"{catalog}.bronze.{source}_records")
    silver = transform(bronze)
    silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{TABLE}")
    return silver.count()
