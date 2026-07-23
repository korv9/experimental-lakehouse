# Databricks notebook source
"""Setup: create the catalog, schemas and control tables (run once per env).

This is Phase-1 platform foundation: nothing dataset-specific, just the
skeleton every source will reuse.
"""
from pyspark.sql import SparkSession

from src.metadata.control_tables import create_platform_tables

CATALOG = "dev_lakehouse"
spark = SparkSession.builder.getOrCreate()

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in ["platform", "bronze", "silver", "gold", "sandbox"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

create_platform_tables(spark, CATALOG)
print("platform ready")
