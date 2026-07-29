# Databricks notebook source
"""SILVER -> GOLD | fashion demand — the dense daily demand panel.

The step that matters is densification. A transactions table only contains days
something sold; aggregate it naively and the model never sees a zero, learns
that demand is always positive, and systematically overstocks. Every calendar
day between a series' first and last sale becomes a row here, with
units_sold = 0 where nothing sold.

The zero share printed below is the check worth reading: if it is near zero,
densification did not happen and the feature layer downstream is built on a
panel with holes.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse_platform.jobs import process_job, read_table

CATALOG = "dev_lakehouse"
ACON = "products/fashion_demand/pipelines/silver_to_gold.yaml"

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

run_id = process_job(spark, acon=ACON, catalog=CATALOG)
print(f"[GOLD] Completed run: {run_id}")

# COMMAND ----------

fact = read_table(spark, "gold.fact_daily_demand", catalog=CATALOG)
dim = read_table(spark, "gold.dim_article", catalog=CATALOG)

rows = fact.count()
zero_rows = fact.where(F.col("units_sold") == 0).count()
series = fact.select("article_id", "sales_channel_id").distinct().count()

print(f"[GOLD] Fact rows:        {rows}")
print(f"[GOLD] Article dim rows: {dim.count()}")
print(f"[GOLD] Distinct series:  {series}")
print(f"[GOLD] Zero-demand rows: {zero_rows} ({zero_rows / max(rows, 1):.1%})")
print("[GOLD] A near-zero share here means densification did not run.")

fact.orderBy("article_id", "demand_date").show(10, truncate=False)
