# Databricks notebook source
"""LANDING -> BRONZE | drug synergy.

Lands the three downloaded files. Point the paths at the real downloads in a
Volume; the defaults are the small repository fixtures so the pipeline is
runnable before anything has been downloaded.
"""
from pyspark.sql import SparkSession

from lakehouse_platform.jobs import process_job

CATALOG = "dev_lakehouse"
ACON = "products/drug_synergy/pipelines/land_bronze.yaml"

# Replace with /Volumes/<catalog>/landing/source_files/... for the real exports.
DRUGCOMB_FILE = "datasets/drug_synergy/drugcombs_scored_sample.csv"
DEPMAP_MODEL_FILE = "datasets/drug_synergy/depmap_model_sample.csv"
DEPMAP_EXPRESSION_FILE = "datasets/drug_synergy/depmap_expression_sample.csv"
DEPMAP_RELEASE = "24Q2"

spark = SparkSession.builder.getOrCreate()

run_id = process_job(
    spark,
    acon=ACON,
    catalog=CATALOG,
    variables={
        "drugcomb_file": DRUGCOMB_FILE,
        "depmap_model_file": DEPMAP_MODEL_FILE,
        "depmap_expression_file": DEPMAP_EXPRESSION_FILE,
        "depmap_release": DEPMAP_RELEASE,
    },
)
print(f"[LANDING] Completed run: {run_id}")
