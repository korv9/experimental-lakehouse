# Databricks notebook source
"""BRONZE -> SILVER | messy records — the platform test bed.

This product is where platform changes get exercised first, so the notebook
uses both notebook-facing entry points end to end:

    read_table   ACON-backed Unity Catalog read (variable resolution + the ACON
                 reader registry), used here to inspect input and verify output
    process_job  runs the ACON graph (read -> clean -> quality -> merge) and
                 records the run in platform.pipeline_runs

The ACON still owns the pipeline itself; the reads around it are what make this
notebook a check rather than a black box.
"""
from pyspark.sql import SparkSession

from lakehouse_platform.jobs import process_job, read_table

CATALOG = "dev_lakehouse"
ACON = "products/messy_records/pipelines/bronze_to_silver.yaml"

BRONZE_TABLE = "bronze.messy_demo_records"
SILVER_TABLE = "silver.records"
QUARANTINE_TABLE = "quarantine.messy_records"

spark = SparkSession.builder.getOrCreate()  # returns the attached Databricks session

# COMMAND ----------
# Before: how much raw data is the ACON about to read?

df_bronze = read_table(spark, BRONZE_TABLE, catalog=CATALOG)
bronze_rows = df_bronze.count()
print(f"[TEST] Bronze rows in: {bronze_rows}")

# COMMAND ----------
# Run: the ACON owns inputs -> transformation -> quality gate -> Delta MERGE.

run_id = process_job(spark, acon=ACON, catalog=CATALOG)
print(f"[TEST] Completed run: {run_id}")

# COMMAND ----------
# After: verify what actually landed, including the rows the gate rejected.

df_silver = read_table(spark, SILVER_TABLE, catalog=CATALOG)
silver_rows = df_silver.count()

quarantine_rows = 0
if spark.catalog.tableExists(f"{CATALOG}.{QUARANTINE_TABLE}"):
    quarantine_rows = read_table(spark, QUARANTINE_TABLE, catalog=CATALOG).count()

print(f"[TEST] Bronze in:   {bronze_rows}")
print(f"[TEST] Silver out:  {silver_rows}")
print(f"[TEST] Quarantined: {quarantine_rows}")
print("[TEST] Silver rows are deduplicated per record_id, so silver <= bronze.")

df_silver.show(5, truncate=False)
