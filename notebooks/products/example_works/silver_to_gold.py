# Databricks notebook source
from lakehouse_platform.jobs import process_job

print("[NOTEBOOK] Starting Example Works Silver -> Kimball Gold")
run_id = process_job(
    spark,  # noqa: F821 - injected by Databricks
    acon="products/example_works/pipelines/silver_to_gold.yaml",
    catalog="dev_lakehouse",
)
print(f"[NOTEBOOK] Completed run: {run_id}")
