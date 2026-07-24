# Databricks notebook: bronze messy_records

import sys

sys.path.append("/Workspace/Repos/experimental-lakehouse")  # adjust to your workspace path

import uuid

from pyspark.sql import functions as F

from lakehouse_framework.orchestration.process_batch import process_job
from lakehouse_framework.read import read_json_records
from lakehouse_framework.schemas.bronze.messy.records import (
    TableDefinition as MessyRecordsBronze,
)


PIPELINE_NAME = "bronze_messy_records"
LANDING_PATH = "/Workspace/Repos/experimental-lakehouse/datasets/messy_demo/raw_records.json"
BATCH_ID = str(uuid.uuid4())


def build_bronze_records():
    # land each raw record verbatim as a JSON string, then attach ingestion metadata
    df = read_json_records(spark, LANDING_PATH)
    return (
        df.withColumn("bk_record_id", F.get_json_object("raw_payload", "$.id"))
        .withColumn("source_name", F.lit("messy_demo"))
        .withColumn("source_endpoint", F.lit("/messy"))
        .withColumn("batch_id", F.lit(BATCH_ID))
        .withColumn("schema_version", F.lit("v1"))
    )
    # dp_ingestion_ts and dp_refresh_ts are injected by process_job.


job_config = {
    "target": {
        "path": MessyRecordsBronze.object_location(),
        "format": "delta",
        "mode": "append",  # bronze is append-only
    },
    "transformation": build_bronze_records,
    "validation": MessyRecordsBronze,
}

process_job(spark, job_config)
