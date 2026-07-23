# Databricks notebook source
"""Ingestion: pull the example API into bronze (append-only)."""
from pyspark.sql import SparkSession

from src.ingestion.ingestion_runner import ingest

spark = SparkSession.builder.getOrCreate()
ingest(spark, "config/sources/example_data.yaml", catalog="dev_lakehouse")
