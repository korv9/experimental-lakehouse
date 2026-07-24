# Databricks notebook source
from lakehouse_platform.engine import run_pipeline

print("[NOTEBOOK] Starting Example Works Silver -> Kimball Gold")
result = run_pipeline(
    spark=spark,  # noqa: F821 - injected by Databricks
    acon="products/example_works/pipelines/silver_to_gold.yaml",
    variables={"catalog": "dev_lakehouse"},
)
print(f"[NOTEBOOK] Completed: {result}")
