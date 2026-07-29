# Databricks notebook source
"""GOLD -> FEATURE | fashion demand — where the ML layer starts.

Feature engineering runs through the same entry point as every other pipeline:
``process_job`` executes the ACON graph (read Gold -> engineer -> quality gate
-> contract -> Delta MERGE) and records the run in ``platform.pipeline_runs``.
A feature table is a governed table, so it earns the same treatment as Silver.

What the notebook adds around the ACON is the check that matters for a training
table: how many rows carry a usable label, and how many are cold starts with no
history yet. Both are visible in ``platform.data_quality_results`` afterwards;
printing them here means a bad build is obvious in the job output.
"""
from pyspark.sql import SparkSession

from lakehouse_platform.jobs import process_job, read_table

CATALOG = "dev_lakehouse"
ACON = "products/fashion_demand/pipelines/gold_to_features.yaml"

GOLD_TABLE = "gold.fact_daily_demand"
FEATURE_TABLE = "feature.demand_features"
QUARANTINE_TABLE = "quarantine.demand_features"

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# Before: the demand panel the features are built from.

df_gold = read_table(spark, GOLD_TABLE, catalog=CATALOG)
gold_rows = df_gold.count()
print(f"[FEATURES] Gold demand rows in: {gold_rows}")

# COMMAND ----------
# Run: lags, rolling windows, calendar, label. All windows end before the row.

run_id = process_job(spark, acon=ACON, catalog=CATALOG)
print(f"[FEATURES] Completed run: {run_id}")

# COMMAND ----------
# After: rows are lost on purpose — the last 14 days of each series have no
# future to be labelled with, so they cannot be trained on.

from pyspark.sql import functions as F  # noqa: E402  (Databricks cell order)

df_features = read_table(spark, FEATURE_TABLE, catalog=CATALOG)
feature_rows = df_features.count()

quarantine_rows = 0
if spark.catalog.tableExists(f"{CATALOG}.{QUARANTINE_TABLE}"):
    quarantine_rows = read_table(spark, QUARANTINE_TABLE, catalog=CATALOG).count()

cold_start_rows = df_features.where(F.col("units_sold_mean_28d").isNull()).count()

print(f"[FEATURES] Gold in:      {gold_rows}")
print(f"[FEATURES] Trainable:    {feature_rows}")
print(f"[FEATURES] Quarantined:  {quarantine_rows}")
print(f"[FEATURES] Cold starts:  {cold_start_rows} rows with no 28-day history")

df_features.select(
    "article_id", "feature_date", "units_sold", "units_sold_mean_7d", "target"
).show(5, truncate=False)
