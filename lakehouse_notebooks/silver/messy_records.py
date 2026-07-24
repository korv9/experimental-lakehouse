# Databricks notebook: silver messy_records

import sys

sys.path.append("/Workspace/Repos/experimental-lakehouse")  # adjust to your workspace path

import json

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

from lakehouse_framework.orchestration.process_batch import process_job
from lakehouse_framework.read import uc_read
from lakehouse_framework.schemas.silver.messy.records import (
    TableDefinition as MessyRecordsSilver,
)
from lakehouse_framework.transform.cleaning import CLEAN_RECORD, clean_record
from lakehouse_framework.transform.hash import dp_fk_hash


PIPELINE_NAME = "silver_messy_records"


@udf(returnType=CLEAN_RECORD)
def clean_udf(raw_payload):
    # one raw JSON string -> fixed, typed struct (schema enforcement via the UDF)
    return clean_record(json.loads(raw_payload))


def build_silver_records():

    bronze = uc_read(spark, "bronze.messy.records")

    # parse + type every raw record; keep bronze ingestion time for lineage + dedup
    parsed = bronze.withColumn("c", clean_udf(F.col("raw_payload"))).select(
        "c.*", "dp_ingestion_ts"
    )

    # quality: error-level rules quarantine rows with no id/title
    good = parsed.filter(F.col("record_id").isNotNull() & F.col("title").isNotNull())

    # dedup: keep the latest row per record_id
    w = Window.partitionBy("record_id").orderBy(F.col("dp_ingestion_ts").desc())
    deduped = good.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")

    # business key + surrogate key (dp_fk_hash convention, same as the fact tables)
    return deduped.withColumnRenamed("record_id", "bk_record_id").withColumn(
        "sk_record", dp_fk_hash("bk_record_id")
    )
    # dp_refresh_ts is injected by process_job; dp_ingestion_ts carried from bronze.


job_config = {
    "target": {
        "path": MessyRecordsSilver.object_location(),
        "format": "delta",
        "mode": "overwrite",
    },
    "transformation": build_silver_records,
    "validation": MessyRecordsSilver,
}

process_job(spark, job_config)
