# Databricks notebook source
from lakehouse_platform.jobs import process_job

print("[NOTEBOOK] Starting Example Works Bronze -> Silver")
run_id = process_job(
    spark,  # noqa: F821 - injected by Databricks
    acon="products/example_works/pipelines/bronze_to_silver.yaml",
    catalog="dev_lakehouse",
)
print(f"[NOTEBOOK] Completed run: {run_id}")
