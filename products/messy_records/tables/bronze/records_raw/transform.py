"""Landing -> Bronze for the messy demo source.

The reader already produced one ``raw_payload`` string per source record. All
this adds is the lineage metadata the contract requires. Nothing is parsed or
cleaned here: interpreting the feed is Silver's job, and Bronze has to stay a
faithful copy so Silver can be rebuilt from it.
"""
from __future__ import annotations

import uuid

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse_platform.observability.progress import progress

SOURCE_NAME = "messy_demo"
SCHEMA_VERSION = "v1"


def transform(landed: DataFrame, options: dict | None = None) -> DataFrame:
    options = options or {}
    source_file = options.get("source_file", "")
    batch_id = options.get("batch_id") or str(uuid.uuid4())
    progress("MESSY_RECORDS", "Adding Bronze lineage metadata", batch_id=batch_id)

    return landed.select(
        F.lit(SOURCE_NAME).alias("source_name"),
        F.lit(source_file).alias("source_file"),
        F.current_timestamp().alias("ingested_at"),
        F.lit(batch_id).alias("batch_id"),
        # the id is read straight out of the payload; it is legitimately missing
        # for some records, which is what the Silver quality gate then rejects
        F.get_json_object("raw_payload", "$.id").alias("source_record_id"),
        F.col("raw_payload"),
        F.lit(SCHEMA_VERSION).alias("schema_version"),
    )
