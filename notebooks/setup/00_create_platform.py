# Databricks notebook source
"""Setup: create the catalog, schemas and control tables (run once per env).

This is Phase-1 platform foundation: nothing dataset-specific, just the
skeleton every source will reuse.
"""
from pyspark.sql import SparkSession

from lakehouse_platform.metadata.control_tables import create_platform_tables
from lakehouse_platform.metadata.unity_catalog import (
    UnityCatalogLayout,
    create_unity_catalog_objects,
)

CATALOG = "dev_lakehouse"
spark = SparkSession.builder.getOrCreate()

layout = UnityCatalogLayout(CATALOG)
create_unity_catalog_objects(spark, layout, create_catalog=True)
create_platform_tables(spark, CATALOG)
print(f"[SETUP] Platform ready: {CATALOG}")
print(f"[SETUP] Raw files: {layout.source_path('shared')}")
print(f"[SETUP] Checkpoints: {layout.checkpoint_path('shared')}")
