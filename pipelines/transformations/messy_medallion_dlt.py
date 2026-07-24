"""Declarative Bronze -> Silver -> Gold for the *messy* source, in DLT.

Shows the idiomatic DLT way to do what src/transformations/bronze_to_silver/
messy_records.py does imperatively:

  * bronze  : Auto Loader lands raw JSON text verbatim (append-only)
  * silver  : a cleaning UDF (the same clean_record used everywhere) enforces the
              schema; @dlt.expect_* gates quality; apply_changes() dedups to the
              latest row per record_id (SCD type 1) — this is how DLT handles the
              duplicate REC-001 without a manual MERGE
  * gold    : an aggregate product

Because the raw feed is heterogeneous (a field may be a string in one row and an
object in the next), bronze keeps the payload as a raw string and the UDF parses
it — a single from_json schema wouldn't fit. ``spark`` and ``dlt`` are provided
by the DLT runtime; there is no main().
"""
import json

import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

from src.schemas.silver.records import CLEAN_RECORD
from src.transformations.cleaning import clean_record


@udf(returnType=CLEAN_RECORD)
def clean_udf(raw_payload: str):
    # one raw JSON string -> the fixed, typed struct (schema enforcement)
    return clean_record(json.loads(raw_payload))


# --- BRONZE: raw JSON text landed in a volume, append-only ---
@dlt.table(comment="Raw messy records (one JSON object per line), append-only")
def bronze_messy_records():
    return (spark.readStream.format("cloudFiles")          # noqa: F821 (spark is provided)
            .option("cloudFiles.format", "text")           # keep the raw JSON string verbatim
            .load("/Volumes/dev/landing/messy_demo/")
            .selectExpr("value AS raw_payload", "current_timestamp() AS ingested_at"))


# --- SILVER (part 1): clean + enforce schema + quality gates ---
# Expectations run here; 'error' rows (null id/title) are dropped before the upsert.
@dlt.view(comment="Cleaned + quality-checked records feeding the SCD upsert")
@dlt.expect_or_drop("record_id_not_null", "record_id IS NOT NULL")
@dlt.expect_or_drop("title_not_null", "title IS NOT NULL")
@dlt.expect("year_in_range", "year IS NULL OR year BETWEEN 0 AND 2100")
@dlt.expect("rating_in_range", "rating IS NULL OR rating BETWEEN 0 AND 5")
def silver_records_clean():
    return (dlt.read_stream("bronze_messy_records")
            .withColumn("c", clean_udf(F.col("raw_payload")))
            .select("c.*", F.col("ingested_at")))


# --- SILVER (part 2): dedup to latest row per record_id via CDC ---
dlt.create_streaming_table("silver_records")
dlt.apply_changes(
    target="silver_records",
    source="silver_records_clean",
    keys=["record_id"],
    sequence_by=F.col("ingested_at"),   # newest ingested_at wins (handles duplicate REC-001)
    stored_as_scd_type=1,               # SCD 1 = overwrite, keep only the current version
)


# --- GOLD: analytics product ---
@dlt.table(comment="Records per category with average rating — dashboard product")
def gold_records_by_category():
    return (dlt.read("silver_records")
            .groupBy("category")
            .agg(F.count("*").alias("record_count"),
                 F.round(F.avg("rating"), 2).alias("avg_rating")))
