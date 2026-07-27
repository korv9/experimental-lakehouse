# Databricks notebook source
"""LANDING -> BRONZE | messy records.

Lands the seed file that ships with the repository into append-only Bronze, so
the rest of the messy pipeline has something to read. Re-running appends another
batch; Silver deduplicates on record_id, so that stays safe.
"""
from pyspark.sql import SparkSession

from lakehouse_platform.jobs import process_job

CATALOG = "dev_lakehouse"
ACON = "products/messy_records/pipelines/land_bronze.yaml"
SOURCE_FILE = "datasets/messy_demo/raw_records.json"

spark = SparkSession.builder.getOrCreate()

run_id = process_job(
    spark,
    acon=ACON,
    catalog=CATALOG,
    variables={"source_file": SOURCE_FILE},
)
print(f"[LANDING] Completed run: {run_id}")
