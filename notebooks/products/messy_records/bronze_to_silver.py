# Databricks notebook source
from lakehouse_platform.jobs import process_job

run_id = process_job(
    spark,  # noqa: F821 - injected by Databricks
    acon="products/messy_records/pipelines/bronze_to_silver.yaml",
    catalog="dev_lakehouse",
)
print(run_id)
