# Databricks notebook source
"""BRONZE -> SILVER | fashion demand — typing, validation and the missing key.

The source has no transaction id. A customer buying the same article twice on
one day is two identical rows, so Silver assigns a deterministic surrogate:
a hash of the identifying fields plus the occurrence number within that group.
Rerunning the same batch reproduces the same keys, which is what makes the
Delta MERGE idempotent instead of duplicating every sale.

Rows failing an error-level rule are quarantined, not dropped, so the loss is
queryable rather than invisible.
"""
from pyspark.sql import SparkSession

from lakehouse_platform.jobs import process_job, read_table

CATALOG = "dev_lakehouse"
ACON = "products/fashion_demand/pipelines/bronze_to_silver.yaml"

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

bronze_rows = read_table(spark, "bronze.hm_transactions_raw", catalog=CATALOG).count()
print(f"[SILVER] Bronze transaction rows in: {bronze_rows}")

# COMMAND ----------

run_id = process_job(spark, acon=ACON, catalog=CATALOG)
print(f"[SILVER] Completed run: {run_id}")

# COMMAND ----------

transactions = read_table(spark, "silver.transactions", catalog=CATALOG)
articles = read_table(spark, "silver.articles", catalog=CATALOG)

quarantined = 0
if spark.catalog.tableExists(f"{CATALOG}.quarantine.hm_transactions"):
    quarantined = read_table(spark, "quarantine.hm_transactions", catalog=CATALOG).count()

print(f"[SILVER] Transactions out: {transactions.count()}")
print(f"[SILVER] Articles out:     {articles.count()}")
print(f"[SILVER] Quarantined:      {quarantined}")

transactions.show(5, truncate=False)
