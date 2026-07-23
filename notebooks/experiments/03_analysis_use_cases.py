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

# --- tiny real example: top categories by total work count ---
top = (spark.table(f"{CATALOG}.gold.analytics_works_by_category")
       .groupBy("category").agg(F.sum("work_count").alias("n"))
       .orderBy(F.desc("n")))
top.show()

# --- example use cases (each would become its own sandbox notebook) ---
#   * K-means / HDBSCAN clustering of works by features
#   * PCA / UMAP dimensionality reduction for visualisation
#   * Text embeddings + topic modelling on titles/descriptions
#   * Network analysis of person <-> work relationships
#   * Anomaly detection on ingestion volumes
#   * Geographical / historical trend analysis
#
# Promote anything durable into a gold pipeline (reproducibility over notebooks).
