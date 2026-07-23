"""Runnable entry point: ingest the example source into bronze.

Wiring only — the logic lives in src/. API ingestion stays imperative (rather
than DLT) because DLT doesn't drive paginated REST pulls; the medallion
transform that follows *is* DLT (see ../transformations/example_medallion_dlt.py).
"""
from pyspark.sql import SparkSession

from src.ingestion.ingestion_runner import ingest

CATALOG = "dev_lakehouse"  # from config/environments/<env>.yaml

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    run_id = ingest(spark, "config/sources/example_data.yaml", catalog=CATALOG)
    print(f"ingestion run complete: {run_id}")
