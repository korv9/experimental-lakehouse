# Databricks notebook source
"""Experimental analysis (sandbox only — never writes to bronze/silver).

Analysis reads stable gold/silver and writes only to the ``sandbox`` schema, so
experiments can't compromise the production layers. Below is one tiny real
example plus a list of the use cases this platform is built to support.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CATALOG = "dev_lakehouse"
spark = SparkSession.builder.getOrCreate()

# Test aggregation over the Kimball star: fact measures grouped by a dimension.
category_metrics = (
    spark.table(f"{CATALOG}.gold.fact_work")
    .join(
        spark.table(f"{CATALOG}.gold.dim_category"),
        on="category_key",
        how="inner",
    )
    .groupBy("category_name")
    .agg(
        F.sum("work_count").alias("work_count"),
        F.sum("tag_count").alias("tag_count"),
    )
    .orderBy(F.desc("work_count"), "category_name")
)

print("[EXPERIMENT] Category aggregation over fact_work + dim_category")
category_metrics.show(truncate=False)

# --- example use cases (each would become its own sandbox notebook) ---
#   * K-means / HDBSCAN clustering of works by features
#   * PCA / UMAP dimensionality reduction for visualisation
#   * Text embeddings + topic modelling on titles/descriptions
#   * Network analysis of person <-> work relationships
#   * Anomaly detection on ingestion volumes
#   * Geographical / historical trend analysis
#
# Promote anything durable into a gold pipeline (reproducibility over notebooks).
