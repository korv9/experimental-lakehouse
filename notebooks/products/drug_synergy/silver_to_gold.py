# Databricks notebook source
"""SILVER -> GOLD | drug synergy star schema."""
from pyspark.sql import SparkSession

from lakehouse_platform.jobs import process_job

CATALOG = "dev_lakehouse"
ACON = "products/drug_synergy/pipelines/silver_to_gold.yaml"

spark = SparkSession.builder.getOrCreate()

run_id = process_job(spark, acon=ACON, catalog=CATALOG)
print(f"[GOLD] Completed run: {run_id}")
