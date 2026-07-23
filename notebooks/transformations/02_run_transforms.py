# Databricks notebook source
"""Transformations (imperative path): bronze -> silver -> gold -> export.

The DLT pipeline (pipelines/transformations/example_medallion_dlt.py) is the
declarative alternative; this notebook shows the same flow step by step so it's
easy to follow.
"""
from pyspark.sql import SparkSession

from src.exports.portfolio_export import export_featured_works
from src.transformations.bronze_to_silver import example_works
from src.transformations.silver_to_gold import works_by_category

CATALOG = "dev_lakehouse"
spark = SparkSession.builder.getOrCreate()

example_works.run(spark, CATALOG)       # bronze -> silver (enforce + DQX + MERGE)
works_by_category.run(spark, CATALOG)   # silver -> gold (aggregate)
path = export_featured_works(spark, CATALOG)   # gold -> JSON for the portfolio
print(f"exported: {path}")
