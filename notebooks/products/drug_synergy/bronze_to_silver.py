# Databricks notebook source
"""BRONZE -> SILVER | drug synergy.

Cleans screens, cell lines, expression, compounds and fingerprints in one graph.

Requires `rdkit` as a cluster library: fingerprints are derived from SMILES here
rather than imported from a CSV, so they are reproducible from Silver.
"""
from pyspark.sql import SparkSession

from lakehouse_platform.jobs import process_job

CATALOG = "dev_lakehouse"
ACON = "products/drug_synergy/pipelines/bronze_to_silver.yaml"

spark = SparkSession.builder.getOrCreate()

run_id = process_job(spark, acon=ACON, catalog=CATALOG)
print(f"[SILVER] Completed run: {run_id}")
