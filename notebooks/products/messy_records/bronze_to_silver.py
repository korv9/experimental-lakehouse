# Databricks notebook source
from lakehouse_platform.engine import run_pipeline

result = run_pipeline(
    spark=spark,  # noqa: F821 - injected by Databricks
    acon="products/messy_records/pipelines/bronze_to_silver.yaml",
    variables={"catalog": "dev_lakehouse"},
)

print(result)
