# Databricks notebook source
"""LANDING -> BRONZE | fashion demand — the H&M Kaggle export.

Run this once per download. The CSVs must already be in the landing Volume;
this notebook does not fetch them, because the Kaggle export needs an
authenticated account and accepting the competition rules, so there is no URL a
job could pull from. See products/fashion_demand/README.md for the download.

Bronze keeps every row verbatim as JSON and types nothing. That is what makes
the next step diagnosable: if a column is renamed upstream, the data still
lands and Silver quarantines what it cannot parse.
"""
from pyspark.sql import SparkSession

from lakehouse_platform.jobs import process_job, read_table

CATALOG = "dev_lakehouse"
ACON = "products/fashion_demand/pipelines/land_bronze.yaml"

# Adjust to wherever the export was uploaded.
LANDING = f"/Volumes/{CATALOG}/landing/source_files/hm"
VARIABLES = {
    "transactions_file": f"{LANDING}/transactions_train.csv",
    "articles_file": f"{LANDING}/articles.csv",
}

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

run_id = process_job(spark, acon=ACON, catalog=CATALOG, variables=VARIABLES)
print(f"[LANDING] Completed run: {run_id}")

# COMMAND ----------

transactions = read_table(spark, "bronze.hm_transactions_raw", catalog=CATALOG)
articles = read_table(spark, "bronze.hm_articles_raw", catalog=CATALOG)

print(f"[LANDING] Bronze transactions: {transactions.count()}")
print(f"[LANDING] Bronze articles:     {articles.count()}")
print("[LANDING] Bronze is append-only, so counts grow by one batch per run.")

transactions.select("source_record_id", "raw_payload").show(3, truncate=False)
